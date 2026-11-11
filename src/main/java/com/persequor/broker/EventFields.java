package com.persequor.broker;

import com.persequor.model.Event;

import java.nio.charset.StandardCharsets;
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
        if (event.getId() == null) {
            return "missing event UUID";
        }
        return invalidTrackedIds(event.getTrackedItemIds());
    }

    static String invalidForStorage(Event event) {
        String invalid = invalidForStatistics(event);
        if (invalid != null) {
            return invalid;
        }
        if (event.getSource() != null && !StandardCharsets.UTF_8.newEncoder().canEncode(event.getSource())) {
            return "source must be valid Unicode";
        }
        for (String id : event.getTrackedItemIds()) {
            if (id.indexOf(0) >= 0 || !StandardCharsets.UTF_8.newEncoder().canEncode(id)
                    || id.getBytes(StandardCharsets.UTF_8).length > 1024) {
                return "tracked item ID must be valid UTF-8 without NUL and at most 1024 bytes";
            }
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
