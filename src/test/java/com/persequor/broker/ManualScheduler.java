package com.persequor.broker;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.Delayed;
import java.util.concurrent.FutureTask;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

final class ManualScheduler extends ScheduledThreadPoolExecutor {
    private final Deque<Task> tasks = new ArrayDeque<>();
    private long now;
    Runnable lastCallback;

    ManualScheduler() {
        super(1);
    }

    long nanoTime() {
        return now;
    }

    void advance(Duration elapsed) {
        elapseWithoutRunning(elapsed);
        while (!tasks.isEmpty() && tasks.peek().due <= now) {
            tasks.remove().run();
        }
    }

    void elapseWithoutRunning(Duration elapsed) {
        now += elapsed.toNanos();
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable task, long delay, TimeUnit unit) {
        if (isShutdown()) {
            throw new java.util.concurrent.RejectedExecutionException("Manual scheduler is closed");
        }
        lastCallback = task;
        Task scheduled = new Task(task, now + unit.toNanos(delay));
        tasks.add(scheduled);
        return scheduled;
    }

    private final class Task extends FutureTask<Void> implements ScheduledFuture<Void> {
        private final long due;

        Task(Runnable task, long due) {
            super(task, null);
            this.due = due;
        }

        @Override
        public long getDelay(TimeUnit unit) {
            return unit.convert(due - now, TimeUnit.NANOSECONDS);
        }

        @Override
        public int compareTo(Delayed other) {
            return Long.compare(getDelay(TimeUnit.NANOSECONDS), other.getDelay(TimeUnit.NANOSECONDS));
        }
    }
}
