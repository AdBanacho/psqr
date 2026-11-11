package com.persequor.broker;

import com.persequor.EventService;
import com.persequor.model.Event;
import com.persequor.model.EventList;
import com.persequor.repository.EventRepository;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.persequor.broker.ListenerFixtures.*;
import static com.persequor.model.EventAction.*;
import static org.junit.Assert.*;

public class ListenerFlowTest {
    private final TestQueue queue = new TestQueue();
    private final MemoryEvents events = new MemoryEvents();
    private final Map<LocalDate, Integer> totals = new LinkedHashMap<>();
    private final ManualScheduler scheduler = new ManualScheduler();
    private final StatisticsListener statistics = new StatisticsListener(queue,
            (day, count) -> totals.merge(day, count, Integer::sum),
            Duration.ofSeconds(1), scheduler, scheduler::nanoTime);
    private final List<Event> subscriptions = new ArrayList<>();

    @Before
    public void wireRealListeners() {
        queue.addListener("persist-queue", new EventStorageListener(events, queue));
        queue.addListener("statistics", statistics);
        queue.addListener("subscription", (event, tag) -> {
            subscriptions.add(event);
            queue.acknowledge(tag);
        });
    }

    @After
    public void shutDown() {
        statistics.close();
        scheduler.shutdown();
    }

    @Test
    public void fullBatchThenTimeoutProcessEachTagExactlyOnceDespiteStaleCallback() {
        List<Event> sent = new ArrayList<>();
        Runnable firstStatisticsTimeout = null;
        for (int i = 0; i < 11; i++) {
            Event event = event(i < 6 ? TIME : TIME.plusMinutes(1), CREATE, "item-" + i, "shared");
            sent.add(event);
            queue.push("persist-queue", event);
            queue.deliver("persist-queue");
            queue.deliver("statistics");
            queue.deliver("subscription");
            if (i == 0) {
                firstStatisticsTimeout = scheduler.lastCallback;
            }
        }

        assertEquals(sent, events.saved);
        assertEquals(sent, subscriptions);
        assertEquals(Integer.valueOf(12), totals.get(TIME.toLocalDate()));
        assertEquals(Integer.valueOf(8), totals.get(TIME.toLocalDate().plusDays(1)));
        assertEquals(32, queue.acknowledged.size());
        assertFalse(queue.acknowledged.contains(32));

        firstStatisticsTimeout.run(); // Simulate a cancelled task that had already started.
        assertFalse(queue.acknowledged.contains(32));
        scheduler.advance(Duration.ofSeconds(1));

        assertEquals(Integer.valueOf(12), totals.get(TIME.toLocalDate()));
        assertEquals(Integer.valueOf(10), totals.get(TIME.toLocalDate().plusDays(1)));
        List<Integer> sortedTags = new ArrayList<>(queue.acknowledged);
        Collections.sort(sortedTags);
        assertEquals(IntStream.rangeClosed(1, 33).boxed().collect(Collectors.toList()), sortedTags);
    }

    @Test
    public void publicEntryPointAcceptsOneEventAndRejectsOneOverlappingOlderEvent() throws Exception {
        EventService service = new EventService(queue);
        Event accepted = event(TIME, SEND, "a", "b");
        Event rejected = event(TIME.minusMinutes(1), CREATE, "b", "c");
        service.handleRequest(accepted);
        queue.deliver("persist-queue");
        queue.deliver("statistics");
        queue.deliver("subscription");
        service.handleRequest(rejected);
        queue.deliver("persist-queue");
        scheduler.advance(Duration.ofSeconds(1));

        assertEquals(Collections.singletonList(accepted), events.saved);
        assertEquals(Collections.singletonList(accepted), subscriptions);
        assertEquals(Collections.singletonMap(TIME.toLocalDate(), 2), totals);
        assertEquals(Arrays.asList(1, 3, 4, 2), queue.acknowledged);
        assertTrue(queue.messages.values().stream().allMatch(Queue::isEmpty));
    }

    private static final class MemoryEvents implements EventRepository {
        private final EventList saved = new EventList();

        @Override
        public void persist(Event event) {
            saved.add(event);
        }

        @Override
        public EventList get(Collection<String> ids) {
            return saved.stream()
                    .filter(event -> !Collections.disjoint(ids, event.getTrackedItemIds()))
                    .collect(Collectors.toCollection(EventList::new));
        }
    }

    private static final class TestQueue implements EventQueue {
        private final Map<String, Queue<Runnable>> messages = new HashMap<>();
        private final Map<String, EventListener> listeners = new HashMap<>();
        private final List<Integer> acknowledged = new ArrayList<>();
        private int nextTag;

        @Override
        public void push(String queue, Event event) {
            int tag = ++nextTag;
            messages.computeIfAbsent(queue, name -> new ArrayDeque<>())
                    .add(() -> listeners.get(queue).handle(event, tag));
        }

        @Override
        public void acknowledge(int tag) {
            acknowledged.add(tag);
        }

        @Override
        public void addListener(String queue, EventListener listener) {
            listeners.put(queue, listener);
        }

        void deliver(String queue) {
            messages.get(queue).remove().run();
        }
    }
}
