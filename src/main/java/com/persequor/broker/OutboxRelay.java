package com.persequor.broker;

import com.persequor.repository.OutboxRepository;
import com.persequor.repository.OutboxRepository.Intent;

import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.logging.Logger;

/** The process supervisor runs exactly one relay for a database and calls poll on one thread. */
public final class OutboxRelay {
    private static final Logger LOG = Logger.getLogger(OutboxRelay.class.getName());

    private final OutboxRepository outbox;
    private final ConfirmedPublisher publisher;
    private final Clock clock;
    private final long timeoutNanos;

    public OutboxRelay(OutboxRepository outbox, ConfirmedPublisher publisher, Clock clock, Duration timeout) {
        this.outbox = outbox;
        this.publisher = publisher;
        this.clock = clock;
        this.timeoutNanos = timeout.toNanos();
        if (timeoutNanos <= 0) {
            throw new IllegalArgumentException("Confirmation timeout must be positive");
        }
    }

    public int poll(int limit) {
        if (limit <= 0) {
            throw new IllegalArgumentException("Poll limit must be positive");
        }
        List<Intent> due = outbox.due(clock.instant(), limit);
        for (Intent intent : due) {
            attempt(intent);
        }
        return due.size();
    }

    private void attempt(Intent intent) {
        try {
            publisher.publish(intent.getDestination(), intent.messageId(), intent.getEvent())
                    .toCompletableFuture().get(timeoutNanos, TimeUnit.NANOSECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted waiting for confirmation of " + intent.messageId(), e);
        } catch (ExecutionException e) {
            retry(intent, e.getCause());
            return;
        } catch (TimeoutException | RuntimeException e) {
            retry(intent, e);
            return;
        }
        // Keep this outside the publish catch. A failed confirmation write leaves the intent recoverable.
        outbox.confirm(intent, clock.instant());
    }

    private void retry(Intent intent, Throwable failure) {
        long delayMillis = Math.min(60_000L, 100L << Math.min(10, intent.getFailureCount()));
        String reason = failure.getClass().getSimpleName();
        outbox.retry(intent, clock.instant().plusMillis(delayMillis), reason);
        LOG.warning("Outbox retry for " + intent.messageId() + " after " + reason
                + " (attempt " + (intent.getFailureCount() + 1L) + ")");
    }
}
