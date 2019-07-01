package com.persequor;

import com.persequor.broker.EventQueue;
import com.persequor.exceptions.EventServiceException;

public class EventService {

    private final EventSerializer eventSerializer;
    private EventQueue eventQueue;

    public EventService(EventSerializer eventSerializer, EventQueue eventQueue)
    {
        this.eventSerializer = eventSerializer;
        this.eventQueue = eventQueue;
    }

    public void handleRequest(String serializedEvent) throws EventServiceException {
        //TODO: Validate event ID
        //TODO: Push it to the queue
    }
}
