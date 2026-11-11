package com.persequor.model;

import java.util.Comparator;
import java.util.Objects;

public final class EventOrdering {
    private static final Comparator<Event> CHRONOLOGY = Comparator
            .comparing(Event::getEventTime)
            .thenComparingInt(event -> event.getAction().getActionOrder());

    private EventOrdering() {
    }

    public static boolean precedes(Event incoming, Event previous) {
        return CHRONOLOGY.compare(incoming, previous) < 0;
    }

    /** Compares events sharing the same UUID. recordTime is ingestion metadata. */
    public static boolean sameContent(Event incoming, Event previous) {
        return incoming.getEventTime().equals(previous.getEventTime())
                && incoming.getAction() == previous.getAction()
                && Objects.equals(incoming.getSource(), previous.getSource())
                && incoming.getTrackedItemIds().equals(previous.getTrackedItemIds());
    }
}
