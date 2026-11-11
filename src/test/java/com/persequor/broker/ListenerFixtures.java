package com.persequor.broker;

import com.persequor.model.Event;
import com.persequor.model.EventAction;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.UUID;

import static org.junit.Assert.fail;

final class ListenerFixtures {
    static final LocalDateTime TIME = LocalDateTime.of(2025, 1, 1, 23, 59);

    private ListenerFixtures() {
    }

    static Event event(LocalDateTime time, EventAction action, String... ids) {
        Event event = new Event();
        event.setId(UUID.randomUUID());
        event.setEventTime(time);
        event.setAction(action);
        event.setTrackedItemIds(Arrays.asList(ids));
        return event;
    }

    static <T extends Throwable> T expect(Class<T> type, Runnable action) {
        try {
            action.run();
        } catch (Throwable failure) {
            if (type.isInstance(failure)) {
                return type.cast(failure);
            }
            throw new AssertionError("Expected " + type.getName(), failure);
        }
        fail("Expected " + type.getName());
        throw new AssertionError();
    }
}
