package com.persequor.broker;

import com.persequor.EventSerializer;
import com.persequor.model.Event;
import com.persequor.repository.EventRepository;

public class EventPersister
	implements EventListener
{
	private final EventRepository repository;
	private EventQueue eventQueue;

	public EventPersister(
			EventRepository repository
			, EventQueue eventQueue
	) {
		this.repository = repository;
		this.eventQueue = eventQueue;
	}

	@Override
	public void handle(Event event, int deliveryTag) {
		//TODO: Store event
		//TODO: Pass event on to "statistics" queue
		//TODO: Acknowledge against the queue
	}
}
