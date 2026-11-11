package com.persequor.repository;

import com.persequor.model.Event;

import java.time.Instant;
import java.util.List;

public interface OutboxRepository {
    List<Intent> due(Instant now, int limit);
    void confirm(Intent intent, Instant confirmedAt);
    void retry(Intent intent, Instant nextAttempt, String error);

    final class Intent {
        private final Event event;
        private final String destination;
        private final int failureCount;

        public Intent(Event event, String destination, int failureCount) {
            this.event = event;
            this.destination = destination;
            this.failureCount = failureCount;
        }

        public Event getEvent() {
            return event;
        }

        public String getDestination() {
            return destination;
        }

        public int getFailureCount() {
            return failureCount;
        }

        public String messageId() {
            return event.getId() + ":" + destination;
        }
    }
}
