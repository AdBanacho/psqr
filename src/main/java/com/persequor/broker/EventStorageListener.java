package com.persequor.broker;

import com.persequor.model.Event;
import com.persequor.repository.EventRepository;

import java.util.Objects;
import java.util.logging.Logger;

public class EventStorageListener implements EventListener {
    private static final Logger LOG = Logger.getLogger(EventStorageListener.class.getName());

    private final EventRepository repository;
    private final EventQueue eventQueue;

    public EventStorageListener(EventRepository repository, EventQueue eventQueue) {
        this.repository = Objects.requireNonNull(repository, "repository");
        this.eventQueue = Objects.requireNonNull(eventQueue, "eventQueue");
    }

    @Override
    public void handle(Event incomingEvent, int deliveryTag) {
        String invalid = EventFields.invalidForStorage(incomingEvent);
        if (invalid != null) {
            LOG.warning("Discarding storage delivery " + deliveryTag + ": " + invalid);
        } else {
            EventRepository.Decision decision = repository.accept(incomingEvent);
            if (decision == EventRepository.Decision.UUID_CONFLICT
                    || decision == EventRepository.Decision.OUT_OF_ORDER) {
                LOG.warning("Discarding storage delivery " + deliveryTag + " for event "
                        + incomingEvent.getId() + ": " + decision);
            }
        }
        eventQueue.acknowledge(deliveryTag);
    }
}
