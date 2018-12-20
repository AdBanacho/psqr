package com.persequor;

import com.google.gson.GsonBuilder;
import com.persequor.model.Event;
import org.junit.Before;
import org.junit.Test;

import java.time.LocalDateTime;
import java.util.UUID;

public class EventSerializerTest {
    private EventSerializer serializer;

    @Before
    public void setup() {
        serializer = new EventSerializer(new GsonBuilder().create());
    }

    /*
     * Write all the tests you need
    @Test
    public void someCoolTestCaseName() throws Throwable {
        Event event = new Event();
        event.setCreatedAt(LocalDateTime.now());
        event.setId(UUID.randomUUID());
        event.setAction("take-over");
        event.setSource("interweb");

        String actual = serializer.serialize(event);
    }

    @Test
    public void someCoolTestCaseName2() throws Throwable {
        Event actual = serializer.deserialize("{id:'4ba28c38-6c9b-4926-b6f5-0d45794fc41f', created_at: '2018-01-12T13:37:30Z', source: 'interweb', action: 'take-over'}");
    }
    */
}
