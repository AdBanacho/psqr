package com.persequor.broker;

import com.persequor.model.Event;
import com.persequor.repository.EventRepository;

public class EventStorageListener
	implements EventListener
{
	private final EventRepository repository;
	private EventQueue eventQueue;

	public EventStorageListener(
			EventRepository repository
			, EventQueue eventQueue
	) {
		this.repository = repository;
		this.eventQueue = eventQueue;
	}

	@Override
	public void handle(Event event, int deliveryTag) {
		//TODO: Validate that events pertaining to the same tracked items are in order
		//      - Handle validation errors
		//TODO: Store event
		//TODO: Pass event on to "statistics" queue and to "subscription" queue
		//TODO: Acknowledge processed events
	}
}
