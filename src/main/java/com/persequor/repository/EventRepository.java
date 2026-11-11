package com.persequor.repository;

import com.persequor.model.Event;

public interface EventRepository {
    enum Decision {
        ACCEPTED, REPLAY, UUID_CONFLICT, OUT_OF_ORDER
    }

    /** Resolves UUID and chronology and commits any event and its publication intents atomically. */
    Decision accept(Event event);
}
