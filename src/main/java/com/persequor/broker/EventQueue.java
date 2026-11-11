package com.persequor.broker;

import com.persequor.model.Event;

public interface EventQueue {
    /** Returns after durable broker acceptance. A failure can leave the outcome unknown. */
    void push(String queue, Event event);

    /**
     * Acknowledges exactly one delivery on the owning channel. Surface adapter failures and
     * preserve redelivery of unacknowledged messages. May be called from a flush worker.
     */
    void acknowledge(int deliveryTag);

    void addListener(String queue, EventListener listener);
}
