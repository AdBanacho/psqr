package com.persequor.broker;

import com.persequor.model.Event;

import java.util.Collections;
import java.util.Comparator;
import java.util.Set;

final class EventOrdering {
    private static final Comparator<Event> CHRONOLOGY = Comparator
            .comparing(Event::getEventTime)
            .thenComparingInt(event -> event.getAction().getActionOrder());

    private EventOrdering() {
    }

    static String rejectionReason(Event incoming, Set<String> trackedIds, Iterable<Event> history) {
        if (history == null) {
            throw new IllegalStateException("Event repository returned null history");
        }
        boolean replay = false;
        boolean older = false;
        for (Event previous : history) {
            if (previous == null || previous.getTrackedItemIds() == null) {
                throw new IllegalStateException("Event repository returned a history row without tracked IDs");
            }
            if (Collections.disjoint(trackedIds, previous.getTrackedItemIds())) {
                continue;
            }
            String invalid = EventFields.invalidForStorage(previous);
            if (invalid != null) {
                throw new IllegalStateException("Event repository returned invalid history: " + invalid);
            }
            replay |= incoming.getId().equals(previous.getId());
            older |= CHRONOLOGY.compare(incoming, previous) < 0;
        }
        if (replay) {
            return "repeated event UUID";
        }
        return older ? "event precedes relevant history" : null;
    }
}
