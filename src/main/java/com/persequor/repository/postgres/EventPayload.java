package com.persequor.repository.postgres;

import com.google.gson.Gson;
import com.persequor.model.Event;
import com.persequor.model.EventAction;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

/** Explicit wire fields keep nanosecond precision and avoid reflective access to Java time classes. */
final class EventPayload {
    private static final Gson JSON = new Gson();

    private UUID id;
    private String eventTime;
    private String recordTime;
    private String source;
    private EventAction action;
    private List<String> trackedItemIds;

    static String encode(Event event) {
        EventPayload payload = new EventPayload();
        payload.id = event.getId();
        payload.eventTime = event.getEventTime().toString();
        payload.recordTime = event.getRecordTime() == null ? null : event.getRecordTime().toString();
        payload.source = event.getSource();
        payload.action = event.getAction();
        payload.trackedItemIds = event.getTrackedItemIds();
        return JSON.toJson(payload);
    }

    static Event decode(String json) {
        EventPayload payload = JSON.fromJson(json, EventPayload.class);
        Event event = new Event();
        event.setId(payload.id);
        event.setEventTime(LocalDateTime.parse(payload.eventTime));
        event.setRecordTime(payload.recordTime == null ? null : LocalDateTime.parse(payload.recordTime));
        event.setSource(payload.source);
        event.setAction(payload.action);
        event.setTrackedItemIds(payload.trackedItemIds);
        return event;
    }
}
