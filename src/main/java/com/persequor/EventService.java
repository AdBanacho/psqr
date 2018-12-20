package com.persequor;

import com.persequor.exceptions.EventServiceException;
import com.persequor.repository.EventRepository;

public class EventService {

    private EventSerializer eventSerializer;
    private EventRepository repository;

    public EventService(EventSerializer eventSerializer, EventRepository repository)
    {
        this.eventSerializer = eventSerializer;
        this.repository = repository;
    }

    public void process(String serializedEvent) throws EventServiceException {
        // deserialize event
        // store event
    }
}
