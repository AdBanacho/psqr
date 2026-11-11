package com.persequor.broker;

import com.persequor.model.Event;
import com.persequor.repository.OutboxRepository;
import org.junit.Test;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.persequor.broker.ListenerFixtures.*;
import static com.persequor.model.EventAction.CREATE;
import static org.junit.Assert.*;

public class OutboxRelayTest {
    @Test
    public void timeoutAndBackoffOnlyRetryTheFailedDestinationWithStableIdentity() {
        Event event = event(TIME, CREATE, "a");
        OutboxRepository.Intent statistics = new OutboxRepository.Intent(event, "statistics", 0);
        OutboxRepository.Intent subscription = new OutboxRepository.Intent(event, "subscription", 0);
        MutableClock clock = new MutableClock(Instant.EPOCH);
        MemoryOutbox outbox = new MemoryOutbox(statistics, subscription);
        List<String> publishes = new ArrayList<>();
        List<Long> deadlines = new ArrayList<>();

        OutboxRelay relay = new OutboxRelay(outbox, (queue, id, payload) -> {
            publishes.add(id);
            assertSame(event, payload);
            if (queue.equals("subscription") && deadlines.isEmpty()) {
                return new CompletableFuture<Void>() {
                    @Override
                    public Void get(long timeout, TimeUnit unit) throws TimeoutException {
                        deadlines.add(unit.toNanos(timeout));
                        throw new TimeoutException("deterministic timeout");
                    }
                };
            }
            return CompletableFuture.completedFuture(null);
        }, clock, Duration.ofMillis(20));

        assertEquals(2, relay.poll(10));
        assertEquals(Arrays.asList(statistics.messageId()), outbox.confirmed);
        assertEquals(Arrays.asList(Duration.ofMillis(20).toNanos()), deadlines);
        assertEquals("TimeoutException", outbox.lastError);
        clock.advance(Duration.ofMillis(99));
        assertEquals(0, relay.poll(10));
        clock.advance(Duration.ofMillis(1));
        assertEquals(1, relay.poll(10));
        assertEquals(Arrays.asList(statistics.messageId(), subscription.messageId(), subscription.messageId()), publishes);
        assertEquals(Arrays.asList(statistics.messageId(), subscription.messageId()), outbox.confirmed);
    }

    private static final class MemoryOutbox implements OutboxRepository {
        private final Map<Intent, Instant> pending = new LinkedHashMap<>();
        private final List<String> confirmed = new ArrayList<>();
        private String lastError;

        private MemoryOutbox(Intent... intents) {
            for (Intent intent : intents) {
                pending.put(intent, Instant.EPOCH);
            }
        }

        @Override
        public List<Intent> due(Instant now, int limit) {
            List<Intent> due = new ArrayList<>();
            pending.forEach((intent, next) -> {
                if (!next.isAfter(now) && due.size() < limit) {
                    due.add(intent);
                }
            });
            return due;
        }

        @Override
        public void confirm(Intent intent, Instant confirmedAt) {
            pending.remove(intent);
            confirmed.add(intent.messageId());
        }

        @Override
        public void retry(Intent intent, Instant nextAttempt, String error) {
            pending.remove(intent);
            pending.put(new Intent(intent.getEvent(), intent.getDestination(), intent.getFailureCount() + 1), nextAttempt);
            lastError = error;
        }
    }
}
