package com.persequor.broker;

import com.persequor.model.Event;

import java.util.List;

final class EventFields {
    private EventFields() {
    }

    static String invalidForStatistics(Event event) {
        if (event == null) {
            return "missing event";
        }
        if (event.getEventTime() == null) {
            return "missing eventTime";
        }
        return invalidTrackedIds(event.getTrackedItemIds());
    }

    static String invalidForStorage(Event event) {
        String invalid = invalidForStatistics(event);
        if (invalid != null) {
            return invalid;
        }
        if (event.getId() == null) {
            return "missing event UUID";
        }
        return event.getAction() == null ? "missing action" : null;
    }

    private static String invalidTrackedIds(List<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return "missing tracked item IDs";
        }
        for (String id : ids) {
            if (id == null || id.trim().isEmpty()) {
                return "blank tracked item ID";
            }
        }
        return null;
    }
}
