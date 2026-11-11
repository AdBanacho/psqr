package com.persequor.broker;

import com.persequor.model.Event;
import com.persequor.repository.EventRepository;
import com.persequor.repository.exceptions.EventRepositoryErrorException;

import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.logging.Logger;

public class EventStorageListener implements EventListener {
    private static final Logger LOG = Logger.getLogger(EventStorageListener.class.getName());

    private final EventRepository repository;
    private final EventQueue eventQueue;

    public EventStorageListener(EventRepository repository, EventQueue eventQueue) {
        this.repository = Objects.requireNonNull(repository, "repository");
        this.eventQueue = Objects.requireNonNull(eventQueue, "eventQueue");
    }

    // One listener instance serializes lookup and persistence for every delivered event.
    @Override
    public synchronized void handle(Event incomingEvent, int deliveryTag) {
        String rejection = EventFields.invalidForStorage(incomingEvent);
        if (rejection == null) {
            Set<String> trackedIds = Collections.unmodifiableSet(new LinkedHashSet<>(incomingEvent.getTrackedItemIds()));
            rejection = EventOrdering.rejectionReason(incomingEvent, trackedIds, repository.get(trackedIds));
        }
        if (rejection != null) {
            LOG.warning("Discarding storage delivery " + deliveryTag + ": " + rejection);
            eventQueue.acknowledge(deliveryTag);
            return;
        }

        try {
            repository.persist(incomingEvent);
        } catch (EventRepositoryErrorException e) {
            throw new IllegalStateException("Could not persist storage delivery " + deliveryTag, e);
        }
        eventQueue.push("statistics", incomingEvent);
        eventQueue.push("subscription", incomingEvent);
        eventQueue.acknowledge(deliveryTag);
    }
}
