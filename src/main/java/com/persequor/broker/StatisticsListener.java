package com.persequor.broker;

import com.persequor.model.Event;
import com.persequor.repository.StatisticsRepository;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.LongSupplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class StatisticsListener implements EventListener, AutoCloseable {
    private static final Logger LOG = Logger.getLogger(StatisticsListener.class.getName());
    private static final int BATCH_SIZE = 10;
    private static final Duration DEFAULT_MAX_AGE = Duration.ofSeconds(1);

    private final EventQueue eventQueue;
    private final StatisticsRepository statisticsRepository;
    private final ScheduledExecutorService scheduler;
    private final long maxAgeNanos;
    private final LongSupplier nanoTime;
    private final boolean ownsScheduler;

    // Access to pending, failure, and closed is serialized by this listener's monitor.
    private Batch pending;
    private IllegalStateException failure;
    private boolean closed;

    public StatisticsListener(EventQueue eventQueue, StatisticsRepository statisticsRepository) {
        this(eventQueue, statisticsRepository, DEFAULT_MAX_AGE, newScheduler(), System::nanoTime, true);
    }

    /** The caller owns the scheduler and keeps it alive until after this listener closes. */
    public StatisticsListener(EventQueue eventQueue, StatisticsRepository statisticsRepository,
                              Duration maxAge, ScheduledExecutorService scheduler) {
        this(eventQueue, statisticsRepository, maxAge, scheduler, System::nanoTime, false);
    }

    StatisticsListener(EventQueue eventQueue, StatisticsRepository statisticsRepository,
                       Duration maxAge, ScheduledExecutorService scheduler, LongSupplier nanoTime) {
        this(eventQueue, statisticsRepository, maxAge, scheduler, nanoTime, false);
    }

    private StatisticsListener(EventQueue eventQueue, StatisticsRepository statisticsRepository,
                               Duration maxAge, ScheduledExecutorService scheduler, LongSupplier nanoTime,
                               boolean ownsScheduler) {
        this.eventQueue = Objects.requireNonNull(eventQueue, "eventQueue");
        this.statisticsRepository = Objects.requireNonNull(statisticsRepository, "statisticsRepository");
        this.scheduler = Objects.requireNonNull(scheduler, "scheduler");
        this.nanoTime = Objects.requireNonNull(nanoTime, "nanoTime");
        this.maxAgeNanos = Objects.requireNonNull(maxAge, "maxAge").toNanos();
        if (maxAgeNanos <= 0) {
            throw new IllegalArgumentException("maxAge must be positive");
        }
        this.ownsScheduler = ownsScheduler;
    }

    private static ScheduledExecutorService newScheduler() {
        ScheduledThreadPoolExecutor executor = new ScheduledThreadPoolExecutor(1, task -> {
            Thread thread = new Thread(task, "statistics-flush");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    @Override
    public synchronized void handle(Event event, int deliveryTag) {
        checkFailure();
        if (closed) {
            throw new IllegalStateException("Statistics listener is closed");
        }
        if (pending != null && nanoTime.getAsLong() - pending.started >= maxAgeNanos) {
            flush();
        }

        String invalid = EventFields.invalidForStatistics(event);
        if (invalid != null) {
            LOG.warning("Discarding statistics delivery " + deliveryTag + ": " + invalid);
            acknowledge(deliveryTag);
            return;
        }

        boolean first = pending == null;
        if (first) {
            pending = new Batch(nanoTime.getAsLong());
        }
        pending.deliveries.add(new Delivery(event.getEventTime().toLocalDate(),
                event.getTrackedItemIds().size(), deliveryTag));
        if (pending.deliveries.size() == BATCH_SIZE) {
            flush();
        } else if (first) {
            Batch batch = pending;
            try {
                long delay = maxAgeNanos - (nanoTime.getAsLong() - batch.started);
                batch.timeout = scheduler.schedule(() -> onTimeout(batch), delay, TimeUnit.NANOSECONDS);
            } catch (RuntimeException e) {
                throw fail("Could not schedule statistics flush", e);
            }
        }
    }

    private synchronized void onTimeout(Batch batch) {
        if (!closed && failure == null && pending == batch) {
            batch.timeout = null;
            flush();
        }
    }

    private void flush() {
        if (pending == null) {
            return;
        }
        pending.cancelTimeout();
        Map<LocalDate, Integer> totals;
        try {
            totals = pending.totals();
        } catch (ArithmeticException e) {
            throw fail("Statistics batch count exceeds the repository's integer range", e);
        }
        int updated = 0;
        for (Map.Entry<LocalDate, Integer> total : totals.entrySet()) {
            try {
                statisticsRepository.updateStatistics(total.getKey(), total.getValue());
                updated++;
            } catch (RuntimeException e) {
                throw fail("Statistics update failed for " + total.getKey() + " after " + updated
                        + " successful day update(s)", e);
            }
        }
        for (Delivery delivery : pending.deliveries) {
            acknowledge(delivery.tag);
        }
        pending = null;
    }

    private void acknowledge(int tag) {
        try {
            eventQueue.acknowledge(tag);
        } catch (RuntimeException e) {
            throw fail("Could not acknowledge statistics delivery " + tag, e);
        }
    }

    private IllegalStateException fail(String message, RuntimeException cause) {
        if (pending != null) {
            pending.cancelTimeout();
        }
        failure = new IllegalStateException(message, cause);
        LOG.log(Level.SEVERE, message, cause);
        return failure;
    }

    private void checkFailure() {
        if (failure != null) {
            throw new IllegalStateException("Statistics listener stopped after a failure", failure);
        }
    }

    /** Stop deliveries before closing. Flushes any healthy partial batch on the calling thread. */
    @Override
    public synchronized void close() {
        try {
            checkFailure();
            if (!closed) {
                flush();
            }
        } finally {
            closed = true;
            if (ownsScheduler) {
                scheduler.shutdown();
            }
        }
    }

    private static final class Batch {
        private final long started;
        private final List<Delivery> deliveries = new ArrayList<>(BATCH_SIZE);
        private ScheduledFuture<?> timeout;

        private Batch(long started) {
            this.started = started;
        }

        private void cancelTimeout() {
            if (timeout != null) {
                timeout.cancel(false);
                timeout = null;
            }
        }

        private Map<LocalDate, Integer> totals() {
            Map<LocalDate, Integer> totals = new LinkedHashMap<>();
            for (Delivery delivery : deliveries) {
                totals.merge(delivery.day, delivery.itemCount, Math::addExact);
            }
            return totals;
        }
    }

    private static final class Delivery {
        private final LocalDate day;
        private final int itemCount;
        private final int tag;

        private Delivery(LocalDate day, int itemCount, int tag) {
            this.day = day;
            this.itemCount = itemCount;
            this.tag = tag;
        }
    }
}
