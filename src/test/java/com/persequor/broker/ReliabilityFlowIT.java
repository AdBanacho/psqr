package com.persequor.broker;

import com.persequor.EventService;
import com.persequor.model.Event;
import com.persequor.repository.postgres.PostgresEventRepository;
import com.persequor.repository.postgres.PostgresOutboxRepository;
import com.persequor.repository.postgres.PostgresStatisticsRepository;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.function.IntConsumer;

import static com.persequor.broker.ListenerFixtures.*;
import static com.persequor.model.EventAction.*;
import static org.junit.Assert.*;

public class ReliabilityFlowIT extends PostgresFixture {
    private final TestBroker broker = new TestBroker();
    private final ManualScheduler scheduler = new ManualScheduler();

    @Test
    public void recoversPublishAndAcknowledgementGapsThroughPublicEntryPoint() throws Exception {
        Event accepted = event(TIME, SEND, "a", "b", "b");
        accepted.setSource("scanner");
        accepted.setRecordTime(TIME.plusNanos(1));
        EventService service = new EventService(broker);
        registerStorage();
        broker.beforeAck = tag -> {
            assertEquals(1, rows("accepted_event"));
            assertEquals(2, rows("event_outbox"));
            assertTrue(broker.publications.isEmpty());
            throw new IllegalStateException("Lost storage ack after commit");
        };

        service.handleRequest(accepted);
        expect(IllegalStateException.class, () -> broker.deliver("persist-queue"));
        assertTrue(broker.acks.isEmpty());

        broker.beforeAck = tag -> { };
        registerStorage();
        broker.redeliver(1);
        assertEquals(2, broker.deliver("persist-queue"));
        service.handleRequest(event(TIME.minusNanos(1), SOLD, "b", "c"));
        assertEquals(3, broker.deliver("persist-queue"));
        assertEquals(Arrays.asList(2, 3), broker.acks);
        assertEquals(1, rows("accepted_event"));
        assertEquals(2, rows("event_outbox"));

        MutableClock clock = new MutableClock(Instant.now().plusSeconds(1));
        broker.failOnce = "subscription";
        assertEquals(2, relay(clock).poll(10));
        assertEquals(1, query("SELECT count(*) FROM event_outbox WHERE confirmed_at IS NOT NULL"));
        assertEquals(1, query("SELECT failure_count FROM event_outbox WHERE destination = 'subscription'"));
        assertEquals(0, relay(clock).poll(10));

        execute("ALTER TABLE event_outbox ADD CONSTRAINT lose_confirmation "
                + "CHECK (confirmed_at IS NULL OR destination <> 'subscription')");
        clock.advance(Duration.ofMillis(100));
        expect(IllegalStateException.class, () -> relay(clock).poll(10));
        assertEquals(1, query("SELECT count(*) FROM event_outbox WHERE confirmed_at IS NOT NULL"));

        execute("ALTER TABLE event_outbox DROP CONSTRAINT lose_confirmation");
        assertEquals(1, relay(clock).poll(10));
        assertEquals(2, query("SELECT count(*) FROM event_outbox WHERE confirmed_at IS NOT NULL"));
        String statisticsId = accepted.getId() + ":statistics";
        String subscriptionId = accepted.getId() + ":subscription";
        assertEquals(Arrays.asList(statisticsId, subscriptionId, subscriptionId), broker.publications);
        List<Event> subscriptions = new ArrayList<>();
        broker.addListener("subscription", (event, tag) -> {
            subscriptions.add(event);
            broker.acknowledge(tag);
        });
        broker.deliver("subscription");
        broker.deliver("subscription");
        for (Event publication : subscriptions) {
            assertEquals(accepted.getId(), publication.getId());
            assertEquals(accepted.getSource(), publication.getSource());
            assertEquals(accepted.getTrackedItemIds(), publication.getTrackedItemIds());
            assertEquals(accepted.getRecordTime(), publication.getRecordTime());
        }
        assertEquals(2, subscriptions.size());

        StatisticsListener original = registerStatistics();
        int statisticsTag = broker.deliver("statistics");
        assertEquals(0, rows("statistics_processed"));
        assertFalse(broker.acks.contains(statisticsTag));
        broker.beforeAck = tag -> {
            assertEquals(statisticsTag, tag);
            assertEquals(1, rows("statistics_processed"));
            assertEquals(3, total(TIME.toLocalDate()));
            throw new IllegalStateException("Lost statistics ack after commit");
        };
        scheduler.advance(Duration.ofSeconds(1));
        expect(IllegalStateException.class, original::close);
        assertFalse(broker.acks.contains(statisticsTag));

        StatisticsListener restarted = registerStatistics();
        broker.beforeAck = tag -> assertEquals(3, total(TIME.toLocalDate()));
        broker.redeliver(statisticsTag);
        int replayTag = broker.deliver("statistics");
        scheduler.advance(Duration.ofSeconds(1));
        restarted.close();

        assertEquals(1, rows("statistics_processed"));
        assertEquals(3, total(TIME.toLocalDate()));
        assertTrue(broker.acks.contains(replayTag));
        assertTrue(broker.inFlight.isEmpty());
    }

    private void registerStorage() {
        broker.addListener("persist-queue", new EventStorageListener(new PostgresEventRepository(database), broker));
    }

    private StatisticsListener registerStatistics() {
        StatisticsListener listener = new StatisticsListener(broker, new PostgresStatisticsRepository(database),
                Duration.ofSeconds(1), scheduler, scheduler::nanoTime);
        broker.addListener("statistics", listener);
        return listener;
    }

    private OutboxRelay relay(MutableClock clock) {
        return new OutboxRelay(new PostgresOutboxRepository(database), broker, clock, Duration.ofMillis(10));
    }

    private static final class TestBroker implements EventQueue, ConfirmedPublisher {
        private final Map<String, Queue<Delivery>> pending = new HashMap<>();
        private final Map<String, EventListener> consumers = new HashMap<>();
        private final Map<Integer, Delivery> inFlight = new HashMap<>();
        private final List<String> publications = new ArrayList<>();
        private final List<Integer> acks = new ArrayList<>();
        private IntConsumer beforeAck = tag -> { };
        private String failOnce;
        private int lastTag;

        @Override
        public void push(String queue, Event event) {
            enqueue(new Delivery(queue, event));
        }

        @Override
        public void acknowledge(int tag) {
            beforeAck.accept(tag);
            assertNotNull(inFlight.remove(tag));
            acks.add(tag);
        }

        @Override
        public void addListener(String queue, EventListener listener) {
            consumers.put(queue, listener);
        }

        @Override
        public CompletionStage<Void> publish(String destination, String messageId, Event event) {
            if (destination.equals(failOnce)) {
                failOnce = null;
                CompletableFuture<Void> failed = new CompletableFuture<>();
                failed.completeExceptionally(new IllegalStateException("Destination unavailable"));
                return failed;
            }
            publications.add(messageId);
            push(destination, event);
            return CompletableFuture.completedFuture(null);
        }

        private int deliver(String queue) {
            int tag = ++lastTag;
            Delivery delivery = pending.get(queue).remove();
            inFlight.put(tag, delivery);
            consumers.get(queue).handle(delivery.event, tag);
            return tag;
        }

        private void redeliver(int failedTag) {
            Delivery delivery = inFlight.remove(failedTag);
            assertNotNull(delivery);
            enqueue(delivery);
        }

        private void enqueue(Delivery delivery) {
            pending.computeIfAbsent(delivery.queue, key -> new ArrayDeque<>()).add(delivery);
        }

        private static final class Delivery {
            private final String queue;
            private final Event event;

            private Delivery(String queue, Event event) {
                this.queue = queue;
                this.event = event;
            }
        }
    }
}
