package com.persequor.broker;

import com.persequor.model.Event;
import com.persequor.model.EventOrdering;
import com.persequor.repository.EventRepository;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.persequor.broker.ListenerFixtures.*;
import static com.persequor.model.EventAction.*;
import static org.junit.Assert.*;

public class EventStorageListenerTest {
    private final List<String> calls = new ArrayList<>();
    private final RuntimeException failure = new RuntimeException("port failure");
    private final EventQueue queue = new EventQueue() {
        @Override
        public void push(String queue, Event event) {
            fail("Storage must publish through the outbox");
        }

        @Override
        public void acknowledge(int tag) {
            calls.add("ack:" + tag);
        }

        @Override
        public void addListener(String queue, EventListener listener) {
            throw new UnsupportedOperationException();
        }
    };

    @Test
    public void orderingUsesTimeThenActionOrderAndIgnoresRecordTime() {
        Event previous = event(TIME, PACKAGE, "a");
        previous.setRecordTime(TIME.plusDays(1));
        assertTrue(EventOrdering.precedes(event(TIME, CREATE, "a"), previous));
        assertTrue(EventOrdering.precedes(event(TIME.minusNanos(1), SOLD, "a"), previous));
        assertFalse(EventOrdering.precedes(event(TIME, PACKAGE, "a"), previous));
        assertFalse(EventOrdering.precedes(event(TIME, SEND, "a"), previous));
        assertFalse(EventOrdering.precedes(event(TIME.plusNanos(1), CREATE, "a"), previous));
    }

    @Test
    public void replayIgnoresRecordTimeAndConflictsOnBusinessFields() {
        Event previous = event(TIME, PACKAGE, "a", "a", "b");
        Event replay = event(TIME, PACKAGE, "a", "a", "b");
        replay.setId(previous.getId());
        replay.setRecordTime(TIME.plusDays(1));
        assertTrue(EventOrdering.sameContent(replay, previous));

        replay.setSource("another source");
        assertFalse(EventOrdering.sameContent(replay, previous));
        replay.setSource(null);
        replay.setTrackedItemIds(Arrays.asList("a", "b"));
        assertFalse(EventOrdering.sameContent(replay, previous));
        replay.setTrackedItemIds(Arrays.asList("b", "a", "a"));
        assertFalse(EventOrdering.sameContent(replay, previous));
    }

    @Test
    public void acknowledgesAfterEveryTerminalRepositoryDecision() {
        Event incoming = event(TIME, CREATE, "a", "b");
        for (EventRepository.Decision decision : EventRepository.Decision.values()) {
            calls.clear();
            EventStorageListener listener = new EventStorageListener(event -> {
                assertSame(incoming, event);
                calls.add("commit");
                return decision;
            }, queue);
            listener.handle(incoming, 7);
            assertEquals(Arrays.asList("commit", "ack:7"), calls);
        }
    }

    @Test
    public void leavesInfrastructureFailuresUnacknowledged() {
        EventStorageListener listener = new EventStorageListener(event -> {
            throw failure;
        }, queue);
        assertSame(failure, expect(RuntimeException.class, () -> listener.handle(event(TIME, CREATE, "a"), 7)));
        assertTrue(calls.isEmpty());
    }

    @Test
    public void invalidEventsAreAcknowledgedWithoutAccessingRepository() {
        EventStorageListener listener = new EventStorageListener(event -> {
            throw new AssertionError("Invalid event reached repository");
        }, queue);
        Event missingUuid = event(TIME, CREATE, "a");
        missingUuid.setId(null);
        Event missingIds = event(TIME, CREATE, "a");
        missingIds.setTrackedItemIds(null);
        List<Event> invalid = Arrays.asList(null, missingUuid, missingIds, event(null, CREATE, "a"),
                event(TIME, null, "a"), event(TIME, CREATE), event(TIME, CREATE, "a", " \t"),
                event(TIME, CREATE, "a", null), event(TIME, CREATE, "a\0b"));
        for (Event event : invalid) {
            listener.handle(event, 7);
        }
        assertEquals(Collections.nCopies(invalid.size(), "ack:7"), calls);
    }
}
