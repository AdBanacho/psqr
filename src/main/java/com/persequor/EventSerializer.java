package com.persequor;

import com.google.gson.Gson;
import com.persequor.model.Event;

public class EventSerializer {
    private final Gson gson;

    public EventSerializer(Gson gson) {
        this.gson = gson;
    }

    public Event deserialize(String serializedEvent) {
        // implement deserializer
        return null;
    }

    public String serialize(Event event) {
        // implement serializer
        return null;
    }
}
