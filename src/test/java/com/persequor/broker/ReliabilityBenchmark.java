package com.persequor.broker;

import com.persequor.EventService;
import com.persequor.model.Event;
import com.persequor.repository.EventRepository.Decision;
import com.persequor.repository.postgres.PostgresEventRepository;
import com.persequor.repository.postgres.PostgresOutboxRepository;
import com.persequor.repository.postgres.PostgresStatisticsRepository;
import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.persequor.broker.ListenerFixtures.TIME;
import static com.persequor.broker.ListenerFixtures.event;
import static com.persequor.model.EventAction.CREATE;
import static org.junit.Assert.assertEquals;

/** Opt in with -Dtest=ReliabilityBenchmark. */
public class ReliabilityBenchmark extends PostgresFixture {
    @Test
    public void measureAcceptToCompletion() throws Exception {
        System.out.printf("JDK %s, %s %s %s, %d processors, PostgreSQL %d, direct JDBC, simulated confirms%n",
                System.getProperty("java.version"), System.getProperty("os.name"), System.getProperty("os.version"),
                System.getProperty("os.arch"), Runtime.getRuntime().availableProcessors(),
                query("SELECT current_setting('server_version_num')::int"));
        System.out.println("Warm-up 200 events at 100/s. 60% hot, 40% multi-item, 80/20 date mix.");
        run("warmup", 200, 10, false);
        run("sparse", 10, 1100, true);
        run("sustained", 1000, 10, true);
        assertEquals(1210, rows("accepted_event"));
        assertEquals(1210, rows("statistics_processed"));
        assertEquals(0, query("SELECT count(*) FROM event_outbox WHERE confirmed_at IS NULL"));
    }

    private void run(String workload, int count, long gapMillis, boolean report) throws Exception {
        BenchmarkBroker broker = new BenchmarkBroker();
        PostgresEventRepository events = new PostgresEventRepository(database);
        PostgresStatisticsRepository statistics = new PostgresStatisticsRepository(database);
        broker.storage = new EventStorageListener(event -> {
            Decision decision = events.accept(event);
            if (decision != Decision.ACCEPTED) {
                throw new AssertionError(decision);
            }
            broker.samples.get(event.getId()).acceptedAt = System.nanoTime();
            return decision;
        }, broker);
        long previousRetries = query("SELECT coalesce(sum(failure_count), 0) FROM event_outbox");
        OutboxRelay relay = new OutboxRelay(new PostgresOutboxRepository(database), broker,
                Clock.systemUTC(), Duration.ofMillis(250));

        ExecutorService executor = Executors.newSingleThreadExecutor();
        AtomicBoolean stopping = new AtomicBoolean();
        try (StatisticsListener consumer = new StatisticsListener(broker, entries -> {
            statistics.apply(entries);
            long committed = System.nanoTime();
            entries.forEach(entry -> broker.samples.get(entry.getId()).statisticsCommittedAt = committed);
        })) {
            Future<Long> peakBacklog = executor.submit(() -> drain(broker, relay, consumer, stopping));
            long start = System.nanoTime();
            EventService service = new EventService(broker);
            try {
                for (int i = 0; i < count; i++) {
                    LocalDateTime time = i % 5 == 4 ? TIME.plusDays(1) : TIME;
                    String item = i % 10 < 6 ? "hot-" + time.toLocalDate() : workload + "-cold-" + i;
                    Event event = i % 10 < 4 ? event(time, CREATE, item, workload + "-second-" + i)
                            : event(time, CREATE, item);
                    broker.samples.put(event.getId(), new Sample(i % 100 == 0));
                    sleepUntil(start + TimeUnit.MILLISECONDS.toNanos(i * gapMillis));
                    service.handleRequest(event);
                }
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
                while (!peakBacklog.isDone() && !broker.samples.values().stream().allMatch(Sample::complete)
                        && System.nanoTime() - deadline < 0) {
                    Thread.sleep(5);
                }
            } finally {
                stopping.set(true);
            }
            long oldestMillis = peakBacklog.get(10, TimeUnit.SECONDS);
            assertEquals(count, broker.samples.values().stream().filter(Sample::complete).count());
            long retries = query("SELECT coalesce(sum(failure_count), 0) FROM event_outbox") - previousRetries;
            if (report) {
                report(workload, start, broker.samples, retries, oldestMillis);
            }
        } finally {
            executor.shutdownNow();
        }
    }

    private long drain(BenchmarkBroker broker, OutboxRelay relay, StatisticsListener statistics,
                       AtomicBoolean stopping) throws InterruptedException {
        long peakBacklog = 0;
        long nextSample = System.nanoTime();
        while (!stopping.get()) {
            if (System.nanoTime() - nextSample >= 0) {
                peakBacklog = Math.max(peakBacklog, query("SELECT coalesce(extract(epoch FROM "
                        + "(clock_timestamp() - min(e.accepted_at))) * 1000, 0)::bigint "
                        + "FROM event_outbox o JOIN accepted_event e ON e.id = o.event_id WHERE o.confirmed_at IS NULL"));
                nextSample = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(100);
            }
            int attempts = relay.poll(40);
            while (!broker.statisticsMessages.isEmpty()) {
                statistics.handle(broker.statisticsMessages.remove(), broker.tags.incrementAndGet());
            }
            if (attempts == 0) {
                Thread.sleep(5);
            }
        }
        return peakBacklog;
    }

    private static void sleepUntil(long target) throws InterruptedException {
        long remaining = target - System.nanoTime();
        if (remaining > 0) {
            TimeUnit.NANOSECONDS.sleep(remaining);
        }
    }

    private static void report(String workload, long start, Map<UUID, Sample> samples, long retries, long oldestMillis) {
        long[] latency = samples.values().stream().mapToLong(sample -> sample.completedAt() - sample.acceptedAt)
                .sorted().toArray();
        long lastAccepted = samples.values().stream().mapToLong(sample -> sample.acceptedAt).max().getAsLong();
        long lastCompleted = samples.values().stream().mapToLong(Sample::completedAt).max().getAsLong();
        long misses = Arrays.stream(latency).filter(value -> value > TimeUnit.SECONDS.toNanos(5)).count();
        System.out.printf(Locale.ROOT, "%s: %d events, %.3f s submit, %.3f s through completion, "
                        + "%.2f accepted/s, %.2f completed/s, p95 %.2f ms, p99 %.2f ms, "
                        + "%d retries, %d ms peak sampled oldest unpublished, %d over 5 s%n",
                workload, latency.length, (lastAccepted - start) / 1e9, (lastCompleted - start) / 1e9,
                latency.length * 1e9 / (lastAccepted - start), latency.length * 1e9 / (lastCompleted - start),
                percentile(latency, .95), percentile(latency, .99), retries, oldestMillis, misses);
    }

    private static double percentile(long[] sorted, double percentile) {
        return sorted[(int) Math.ceil(percentile * sorted.length) - 1] / 1e6;
    }

    private static final class Sample {
        private volatile long acceptedAt;
        private volatile long statisticsConfirmedAt;
        private volatile long subscriptionConfirmedAt;
        private volatile long statisticsCommittedAt;
        private boolean rejectFirstSubscription;

        private Sample(boolean rejectFirstSubscription) {
            this.rejectFirstSubscription = rejectFirstSubscription;
        }

        private boolean complete() {
            return acceptedAt != 0 && statisticsConfirmedAt != 0 && subscriptionConfirmedAt != 0
                    && statisticsCommittedAt != 0;
        }

        private long completedAt() {
            return Math.max(statisticsCommittedAt, Math.max(statisticsConfirmedAt, subscriptionConfirmedAt));
        }
    }

    private static final class BenchmarkBroker implements EventQueue, ConfirmedPublisher {
        private final Map<UUID, Sample> samples = new ConcurrentHashMap<>();
        private final Queue<Event> statisticsMessages = new ArrayDeque<>(); // only accessed by relay worker
        private final AtomicInteger tags = new AtomicInteger();
        private EventStorageListener storage;

        @Override
        public void push(String destination, Event event) {
            storage.handle(event, tags.incrementAndGet());
        }

        @Override
        public void acknowledge(int tag) {
            // Successful in-process acknowledgement.
        }

        @Override
        public void addListener(String destination, EventListener listener) {
            throw new UnsupportedOperationException();
        }

        @Override
        public CompletionStage<Void> publish(String destination, String messageId, Event event) {
            Sample sample = samples.get(event.getId());
            if ("statistics".equals(destination)) {
                sample.statisticsConfirmedAt = System.nanoTime();
                statisticsMessages.add(event);
            } else if (sample.rejectFirstSubscription) {
                sample.rejectFirstSubscription = false;
                CompletableFuture<Void> failed = new CompletableFuture<>();
                failed.completeExceptionally(new IllegalStateException("Simulated publisher rejection"));
                return failed;
            } else {
                sample.subscriptionConfirmedAt = System.nanoTime();
            }
            return CompletableFuture.completedFuture(null);
        }
    }
}
