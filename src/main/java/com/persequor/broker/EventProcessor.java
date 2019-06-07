package com.persequor.broker;

import com.persequor.EventSerializer;
import com.persequor.repository.EventRepository;

public class EventProcessor
	implements EventListener
{
	private final EventRepository repository;
	private final EventSerializer serializer;
	private EventQueue eventQueue;

	public EventProcessor(
			EventRepository repository
			, EventSerializer serializer
			, EventQueue eventQueue
	) {
		this.repository = repository;
		this.serializer = serializer;
		this.eventQueue = eventQueue;
	}

	@Override
	public void handle(String event) {
		//TODO: Deserialize event
		//TODO: Store event
		//TODO: Acknowledge against the queue
	}
}
