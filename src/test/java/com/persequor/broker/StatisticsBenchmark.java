package com.persequor.broker;

import com.persequor.model.Event;
import org.junit.Test;

import java.util.Arrays;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.persequor.broker.ListenerFixtures.TIME;
import static com.persequor.broker.ListenerFixtures.event;
import static com.persequor.model.EventAction.CREATE;

/** Opt-in with -Dtest=StatisticsBenchmark. Uses the real scheduler and no production adapters. */
public class StatisticsBenchmark {
    private static final long SPARSE_GAP_NANOS = TimeUnit.MILLISECONDS.toNanos(1100);

    @Test
    public void measureArrivalToAcknowledgement() throws InterruptedException {
        System.out.printf("JDK %s (%s), %s %s %s, %d available processors%n",
                System.getProperty("java.version"), System.getProperty("java.vendor"),
                System.getProperty("os.name"), System.getProperty("os.version"),
                System.getProperty("os.arch"), Runtime.getRuntime().availableProcessors());
        System.out.println("In-process queue and statistics counter, 2 items/event, dates 80/20, default 1 s age");
        System.out.println("Warm-up: 3 sparse deliveries, then 50,000 sustained deliveries");
        run(3, SPARSE_GAP_NANOS);
        run(50_000, 0);

        run(10, SPARSE_GAP_NANOS).report("sparse (1,100 ms arrival gap)");
        run(200_000, 0).report("sustained (no arrival gap)");
    }

    private static Measurement run(int count, long gapNanos) throws InterruptedException {
        Measurement measurement = new Measurement(count);
        Event[] pattern = new Event[10];
        for (int i = 0; i < pattern.length; i++) {
            pattern[i] = event(i < 8 ? TIME : TIME.plusMinutes(1), CREATE, "a", "b");
        }
        try (StatisticsListener listener = new StatisticsListener(measurement, (day, items) -> {
            measurement.writesSinceAck++;
            measurement.itemsWritten += items;
        })) {
            long start = System.nanoTime();
            for (int tag = 0; tag < count; tag++) {
                long remaining = start + tag * gapNanos - System.nanoTime();
                if (remaining > 0) {
                    TimeUnit.NANOSECONDS.sleep(remaining);
                }
                measurement.arrivals[tag] = System.nanoTime();
                listener.handle(pattern[tag % pattern.length], tag);
            }
            if (!measurement.acks.await(60, TimeUnit.SECONDS)) {
                throw new AssertionError("Benchmark did not acknowledge all deliveries");
            }
            measurement.elapsed = measurement.lastAck - measurement.arrivals[0];
            if (measurement.itemsWritten != 2 * count) {
                throw new AssertionError("Unexpected statistics item count");
            }
        }
        return measurement;
    }

    private static final class Measurement implements EventQueue {
        private final long[] arrivals;
        private final long[] latency;
        private final CountDownLatch acks;
        private long elapsed;
        private long lastAck;
        private int itemsWritten;
        private int writesSinceAck;
        private int writes;
        private int batches;

        private Measurement(int count) {
            arrivals = new long[count];
            latency = new long[count];
            acks = new CountDownLatch(count);
        }

        @Override
        public void acknowledge(int tag) {
            lastAck = System.nanoTime();
            latency[tag] = lastAck - arrivals[tag];
            if (writesSinceAck > 0) {
                batches++;
                writes += writesSinceAck;
                writesSinceAck = 0;
            }
            acks.countDown();
        }

        private void report(String workload) {
            Arrays.sort(latency);
            int p95 = (int) Math.ceil(latency.length * 0.95) - 1;
            System.out.printf(Locale.ROOT, "%s: %,d events, %,d batches, %.2f events/s, "
                            + "p95 %.3f ms, %.2f writes/batch%n",
                    workload, latency.length, batches, latency.length * 1e9 / elapsed,
                    latency[p95] / 1e6, (double) writes / batches);
        }

        @Override
        public void push(String queue, Event event) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void addListener(String queue, EventListener listener) {
            throw new UnsupportedOperationException();
        }
    }
}
