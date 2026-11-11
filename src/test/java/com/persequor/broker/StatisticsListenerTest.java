package com.persequor.broker;

import com.persequor.model.Event;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.persequor.broker.ListenerFixtures.*;
import static com.persequor.model.EventAction.CREATE;
import static org.junit.Assert.*;

public class StatisticsListenerTest {
    private static final LocalDate DAY = TIME.toLocalDate();
    private static final LocalDate NEXT_DAY = DAY.plusDays(1);
    private final ManualScheduler scheduler = new ManualScheduler();
    private final List<String> calls = new ArrayList<>();
    private final RuntimeException portFailure = new RuntimeException("port failure");
    private LocalDate failingDay;
    private int failingTag = -1;
    private final EventQueue queue = new EventQueue() {
        @Override
        public void acknowledge(int tag) {
            calls.add("ack:" + tag);
            if (tag == failingTag) {
                throw portFailure;
            }
        }

        @Override
        public void addListener(String name, EventListener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void push(String name, Event event) {
            throw new UnsupportedOperationException();
        }
    };
    private final StatisticsListener listener = new StatisticsListener(queue, this::update,
            Duration.ofSeconds(1), scheduler, scheduler::nanoTime);

    @Test
    public void nineWaitTenFlushAndElevenStartsANewBatch() {
        Event event = event(TIME, CREATE, "a", "a");
        for (int tag = 1; tag <= 9; tag++) {
            listener.handle(event, tag);
        }
        assertTrue(calls.isEmpty());

        listener.handle(event, 10);
        List<String> expected = new ArrayList<>();
        expected.add(DAY + ":20");
        for (int tag = 1; tag <= 10; tag++) {
            expected.add("ack:" + tag);
        }
        assertEquals(expected, calls);

        listener.handle(event, 11);
        assertEquals(expected, calls);
        scheduler.advance(Duration.ofSeconds(1));
        expected.add(DAY + ":2");
        expected.add("ack:11");
        assertEquals(expected, calls);
    }

    @Test
    public void deadlineIsMeasuredFromFirstDeliveryAndGroupsLocalDates() {
        listener.handle(event(TIME, CREATE, "a", "b"), 1);
        scheduler.advance(Duration.ofMillis(900));
        listener.handle(event(TIME.plusMinutes(1), CREATE, "a", "b", "c"), 2);
        listener.handle(event(TIME, CREATE, "a"), 3);
        scheduler.advance(Duration.ofMillis(99));
        assertTrue(calls.isEmpty());

        scheduler.advance(Duration.ofMillis(1));

        assertEquals(Arrays.asList(DAY + ":3", NEXT_DAY + ":3", "ack:1", "ack:2", "ack:3"), calls);
    }

    @Test
    public void deliveryAtDeadlineFlushesTheOldBatchEvenIfSchedulerIsLate() {
        listener.handle(event(TIME, CREATE, "a"), 1);
        scheduler.elapseWithoutRunning(Duration.ofSeconds(1));
        listener.handle(event(TIME, CREATE, "b"), 2);
        assertEquals(Arrays.asList(DAY + ":1", "ack:1"), calls);

        scheduler.advance(Duration.ofMillis(999));
        assertEquals(Arrays.asList(DAY + ":1", "ack:1"), calls);
        scheduler.advance(Duration.ofMillis(1));
        assertEquals(Arrays.asList(DAY + ":1", "ack:1", DAY + ":1", "ack:2"), calls);
    }

    @Test
    public void closeFlushesPendingOnceAndLeavesBorrowedSchedulerRunning() {
        listener.handle(event(TIME, CREATE, "a"), 1);
        Runnable stale = scheduler.lastCallback;
        listener.close();
        listener.close();
        stale.run();
        scheduler.advance(Duration.ofSeconds(1));

        assertEquals(Arrays.asList(DAY + ":1", "ack:1"), calls);
        assertFalse(scheduler.isShutdown());
        expect(IllegalStateException.class, () -> listener.handle(event(TIME, CREATE, "b"), 2));
    }

    @Test
    public void invalidFieldsAreAcknowledgedWithoutChangingBatchAndUnusedFieldsAreOptional() {
        Event missingList = event(TIME, CREATE, "a");
        missingList.setTrackedItemIds(null);
        List<Event> invalid = Arrays.asList(null, missingList, event(null, CREATE, "a"),
                event(TIME, CREATE), event(TIME, CREATE, "a", null), event(TIME, CREATE, "  "));
        for (Event event : invalid) {
            listener.handle(event, 1);
        }
        assertEquals(Collections.nCopies(invalid.size(), "ack:1"), calls);
        calls.clear();

        Event valid = event(TIME, null, "a");
        valid.setId(null);
        listener.handle(valid, 2);
        listener.close();

        assertEquals(Arrays.asList(DAY + ":1", "ack:2"), calls);
    }

    @Test
    public void partialMultiDayFailureLeavesAllTagsUnacknowledgedAndNeverRepeatsUpdates() {
        failingDay = NEXT_DAY;
        listener.handle(event(TIME, CREATE, "a"), 1);
        listener.handle(event(TIME.plusMinutes(1), CREATE, "b", "c"), 2);

        scheduler.advance(Duration.ofSeconds(1));

        IllegalStateException stopped = expect(IllegalStateException.class,
                () -> listener.handle(event(TIME, CREATE, "c"), 3));
        assertSame(portFailure, stopped.getCause().getCause());
        assertTrue(stopped.getCause().getMessage().contains("after 1 successful day"));
        expect(IllegalStateException.class, listener::close);
        scheduler.advance(Duration.ofSeconds(1));

        assertEquals(Arrays.asList(DAY + ":1", NEXT_DAY + ":2"), calls);
    }

    @Test
    public void acknowledgementFailureStopsBeforeLaterTagsAndDoesNotRepeatTheUpdates() {
        failingTag = 2;
        for (int tag = 1; tag <= 3; tag++) {
            listener.handle(event(TIME, CREATE, "a"), tag);
        }

        IllegalStateException failure = expect(IllegalStateException.class, listener::close);
        assertSame(portFailure, failure.getCause());
        expect(IllegalStateException.class, listener::close);
        scheduler.advance(Duration.ofSeconds(1));
        assertEquals(Arrays.asList(DAY + ":3", "ack:1", "ack:2"), calls);
    }

    @Test
    public void rejectedSchedulingIsObservableAndLeavesPendingTagUnacknowledged() {
        scheduler.shutdown();
        IllegalStateException failure = expect(IllegalStateException.class,
                () -> listener.handle(event(TIME, CREATE, "a"), 1));
        assertTrue(failure.getCause() instanceof RejectedExecutionException);
        expect(IllegalStateException.class, listener::close);
        assertTrue(calls.isEmpty());
    }

    @Test
    public void customMaximumAgeControlsPartialFlush() {
        AtomicBoolean flushed = new AtomicBoolean();
        StatisticsListener custom = new StatisticsListener(queue, (day, count) -> flushed.set(true),
                Duration.ofMillis(75), scheduler, scheduler::nanoTime);
        custom.handle(event(TIME, CREATE, "a"), 1);
        scheduler.advance(Duration.ofMillis(74));
        assertFalse(flushed.get());
        scheduler.advance(Duration.ofMillis(1));
        assertTrue(flushed.get());
        custom.close();
    }

    private void update(LocalDate day, int count) {
        calls.add(day + ":" + count);
        if (day.equals(failingDay)) {
            throw portFailure;
        }
    }
}
