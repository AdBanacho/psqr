package com.persequor.broker;

import com.persequor.model.Event;

import java.util.concurrent.CompletionStage;

public interface ConfirmedPublisher {
    /**
     * Submits promptly and completes successfully only on durable, routed broker confirmation.
     * Rejection, unroutability and transport failure complete exceptionally. A timed-out wait
     * leaves acceptance uncertain. The adapter retires abandoned correlation state.
     */
    CompletionStage<Void> publish(String destination, String messageId, Event event);
}
