package com.persequor.broker;

import com.persequor.model.Event;
import com.persequor.model.EventList;
import com.persequor.repository.EventRepository;
import com.persequor.repository.exceptions.EventRepositoryErrorException;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.persequor.broker.ListenerFixtures.*;
import static com.persequor.model.EventAction.*;
import static org.junit.Assert.*;

public class EventStorageListenerTest {
    private final RecordingPorts ports = new RecordingPorts();
    private final EventStorageListener listener = new EventStorageListener(ports, ports);

    @Test
    public void anyOverlapWithLaterUnsortedHistoryRejectsTheWholeEvent() {
        ports.history.add(event(TIME.minusMinutes(1), SOLD, "a"));
        ports.history.add(event(TIME.plusMinutes(1), CREATE, "b"));
        ports.history.add(event(TIME.minusMinutes(2), SOLD, "b"));
        listener.handle(event(TIME, CREATE, "a", "b", "b"), 7);

        assertEquals(new HashSet<>(Arrays.asList("a", "b")), new HashSet<>(ports.lookup));
        assertEquals(2, ports.lookup.size());
        assertEquals(Arrays.asList("lookup", "ack:7"), ports.calls);
    }

    @Test
    public void eventTimeWinsAndUnrelatedHistoryIsIgnored() {
        Event previous = event(TIME.minusMinutes(1), SOLD, "a");
        previous.setRecordTime(TIME.plusDays(2));
        ports.history.add(previous);
        ports.history.add(event(TIME.plusDays(3), SOLD, "unrelated"));
        Event incoming = event(TIME, CREATE, "a");
        incoming.setRecordTime(TIME.minusDays(2));

        listener.handle(incoming, 7);

        assertSame(incoming, ports.saved);
        assertEquals(Arrays.asList("lookup", "persist", "statistics", "subscription", "ack:7"), ports.calls);
    }

    @Test
    public void equalTimesUseActionOrderAndAllowDistinctEqualEvents() {
        ports.history.add(event(TIME, PACKAGE, "a"));
        listener.handle(event(TIME, CREATE, "a"), 7);
        listener.handle(event(TIME, PACKAGE, "a"), 8);
        listener.handle(event(TIME, SEND, "a"), 9);

        assertEquals(Arrays.asList("lookup", "ack:7",
                "lookup", "persist", "statistics", "subscription", "ack:8",
                "lookup", "persist", "statistics", "subscription", "ack:9"), ports.calls);
    }

    @Test
    public void uuidReplayInRelevantHistoryIsAcknowledgedWithoutRepublishing() {
        Event saved = event(TIME, CREATE, "a");
        ports.history.add(saved);
        ports.history.add(event(TIME.plusHours(1), SOLD, "a"));
        Event replay = event(TIME.plusHours(2), PACKAGE, "a", "b");
        replay.setId(saved.getId());

        listener.handle(replay, 7);

        assertEquals(Arrays.asList("lookup", "ack:7"), ports.calls);
    }

    @Test
    public void uuidInDisjointHistoryIsOutsideReplayLookup() {
        Event saved = event(TIME, CREATE, "a");
        ports.history.add(saved);
        Event incoming = event(TIME, CREATE, "b");
        incoming.setId(saved.getId());

        listener.handle(incoming, 7);

        assertSame(incoming, ports.saved);
    }

    @Test
    public void missingFieldsAndInvalidItemIdsAreAcknowledgedWithoutLookup() {
        Event noUuid = event(TIME, CREATE, "a");
        noUuid.setId(null);
        Event noIds = event(TIME, CREATE, "a");
        noIds.setTrackedItemIds(null);
        List<Event> invalid = Arrays.asList(null, noUuid, noIds, event(null, CREATE, "a"),
                event(TIME, null, "a"), event(TIME, CREATE), event(TIME, CREATE, "a", " \t"),
                event(TIME, CREATE, "a", null));
        for (Event event : invalid) {
            listener.handle(event, 7);
        }
        assertEquals(Collections.nCopies(invalid.size(), "ack:7"), ports.calls);
    }

    @Test
    public void invalidRelevantHistoryIsAnInfrastructureFailure() {
        ports.history.add(event(null, CREATE, "a"));
        expect(IllegalStateException.class, () -> listener.handle(event(TIME, CREATE, "a"), 7));
        assertEquals(Collections.singletonList("lookup"), ports.calls);
    }

    @Test
    public void runtimeFailuresStopSideEffectsAtTheFailingStep() {
        List<String> steps = Arrays.asList("lookup", "persist", "statistics", "subscription", "ack:7");
        for (int i = 0; i < steps.size(); i++) {
            ports.calls.clear();
            ports.failAt = steps.get(i);
            assertSame(ports.failure, expect(RuntimeException.class,
                    () -> listener.handle(event(TIME, CREATE, "a"), 7)));
            assertEquals(steps.subList(0, i + 1), ports.calls);
        }
    }

    @Test
    public void checkedPersistenceFailureKeepsItsCauseAndLeavesDeliveryUnacknowledged() {
        ports.checkedFailure = new EventRepositoryErrorException();
        IllegalStateException failure = expect(IllegalStateException.class,
                () -> listener.handle(event(TIME, CREATE, "a"), 7));
        assertSame(ports.checkedFailure, failure.getCause());
        assertEquals(Arrays.asList("lookup", "persist"), ports.calls);
    }

    @Test
    public void oneListenerSerializesOverlappingLookupsAndPersistence() throws Exception {
        CountDownLatch insidePersist = new CountDownLatch(1);
        CountDownLatch releasePersist = new CountDownLatch(1);
        CountDownLatch olderStarted = new CountDownLatch(1);
        AtomicReference<Thread> olderThread = new AtomicReference<>();
        Event newer = event(TIME, SEND, "a", "b");
        Event older = event(TIME.minusMinutes(1), CREATE, "b");
        RecordingPorts gated = new RecordingPorts() {
            @Override
            public void persist(Event event) throws EventRepositoryErrorException {
                if (event == newer) {
                    insidePersist.countDown();
                    await(releasePersist);
                }
                super.persist(event);
                history.add(event);
            }
        };
        EventStorageListener singleConsumer = new EventStorageListener(gated, gated);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        try {
            Future<?> first = workers.submit(() -> singleConsumer.handle(newer, 7));
            await(insidePersist);
            Future<?> second = workers.submit(() -> {
                olderThread.set(Thread.currentThread());
                olderStarted.countDown();
                singleConsumer.handle(older, 8);
            });
            await(olderStarted);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (!second.isDone() && olderThread.get().getState() != Thread.State.BLOCKED
                    && System.nanoTime() - deadline < 0) {
                Thread.yield();
            }
            assertEquals(Thread.State.BLOCKED, olderThread.get().getState());
            releasePersist.countDown();
            first.get(5, TimeUnit.SECONDS);
            second.get(5, TimeUnit.SECONDS);
            assertEquals(Collections.singletonList(newer), gated.history);
            assertEquals(Arrays.asList("lookup", "persist", "statistics", "subscription", "ack:7",
                    "lookup", "ack:8"), gated.calls);
        } finally {
            releasePersist.countDown();
            workers.shutdownNow();
        }
    }

    private static void await(CountDownLatch latch) {
        try {
            assertTrue("Timed out waiting for test worker", latch.await(5, TimeUnit.SECONDS));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    private static class RecordingPorts implements EventRepository, EventQueue {
        final List<String> calls = new ArrayList<>();
        final EventList history = new EventList();
        final RuntimeException failure = new RuntimeException("port failure");
        EventRepositoryErrorException checkedFailure;
        Collection<String> lookup;
        Event saved;
        String failAt;

        @Override
        public EventList get(Collection<String> ids) {
            lookup = ids;
            record("lookup");
            return history;
        }

        @Override
        public void persist(Event event) throws EventRepositoryErrorException {
            record("persist");
            if (checkedFailure != null) {
                throw checkedFailure;
            }
            saved = event;
        }

        @Override
        public void push(String queue, Event event) {
            assertSame(saved, event);
            record(queue);
        }

        @Override
        public void acknowledge(int tag) {
            record("ack:" + tag);
        }

        @Override
        public void addListener(String queue, EventListener listener) {
            throw new UnsupportedOperationException();
        }

        private void record(String step) {
            calls.add(step);
            if (step.equals(failAt)) {
                throw failure;
            }
        }
    }
}
