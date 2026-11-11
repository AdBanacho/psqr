package com.persequor.broker;

import com.persequor.model.Event;
import com.persequor.model.StatisticsEntry;
import com.persequor.repository.postgres.PostgresEventRepository;
import com.persequor.repository.postgres.PostgresStatisticsRepository;
import org.junit.Test;
import org.postgresql.ds.PGSimpleDataSource;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLTransientConnectionException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static com.persequor.broker.ListenerFixtures.*;
import static com.persequor.model.EventAction.*;
import static com.persequor.repository.EventRepository.Decision.*;
import static org.junit.Assert.*;

public class PostgresTransactionsIT extends PostgresFixture {
    @Test
    public void rollsBackEventHeadsAndBothIntentsIfOutboxInsertFails() throws SQLException {
        execute("ALTER TABLE event_outbox ADD CONSTRAINT reject_subscription CHECK (destination <> 'subscription')");
        PostgresEventRepository storage = new PostgresEventRepository(database);
        Event event = event(TIME, CREATE, "a", "b", "b");

        expect(IllegalStateException.class, () -> storage.accept(event));
        assertEquals(0, rows("accepted_event"));
        assertEquals(0, rows("tracked_item"));
        assertEquals(0, rows("event_outbox"));

        execute("ALTER TABLE event_outbox DROP CONSTRAINT reject_subscription");
        assertEquals(ACCEPTED, storage.accept(event));
        assertEquals(2, rows("tracked_item"));
        assertEquals(2, rows("event_outbox"));
    }

    @Test
    public void redeliveryResolvesUnknownCommitThroughDurableUuid() {
        PGSimpleDataSource loseCommitResponse = new PGSimpleDataSource() {
            @Override
            public Connection getConnection() throws SQLException {
                Connection delegate = database.getConnection();
                return (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                        new Class<?>[]{Connection.class}, (proxy, method, args) -> {
                            Object result;
                            try {
                                result = method.invoke(delegate, args);
                            } catch (InvocationTargetException e) {
                                throw e.getCause();
                            }
                            if (method.getName().equals("commit")) {
                                throw new SQLTransientConnectionException("Connection lost after commit", "08006");
                            }
                            return result;
                        });
            }
        };
        Event event = event(TIME, CREATE, "a");
        expect(IllegalStateException.class, () -> new PostgresEventRepository(loseCommitResponse).accept(event));
        assertEquals(REPLAY, new PostgresEventRepository(database).accept(event));
        assertEquals(1, rows("accepted_event"));
        assertEquals(2, rows("event_outbox"));
    }

    @Test
    public void preservesNanosecondOrderingAndDetectsGlobalUuidConflictBeforeChronology() {
        PostgresEventRepository storage = new PostgresEventRepository(database);
        Event first = event(TIME.plusNanos(2), PACKAGE, "a");
        assertEquals(ACCEPTED, storage.accept(first));
        assertEquals(OUT_OF_ORDER, storage.accept(event(TIME.plusNanos(1), SOLD, "a", "b")));
        assertEquals(OUT_OF_ORDER, storage.accept(event(TIME.plusNanos(2), CREATE, "a")));
        assertEquals(ACCEPTED, storage.accept(event(TIME.plusNanos(2), PACKAGE, "a")));
        assertEquals(ACCEPTED, storage.accept(event(TIME.plusNanos(3), CREATE, "a")));

        Event replay = event(first.getEventTime(), PACKAGE, "a");
        replay.setId(first.getId());
        replay.setRecordTime(TIME.plusDays(1));
        assertEquals(REPLAY, storage.accept(replay));
        replay.setTrackedItemIds(Collections.singletonList("disjoint"));
        assertEquals(UUID_CONFLICT, storage.accept(replay));
        assertEquals(3, rows("accepted_event"));
        assertEquals(6, rows("event_outbox"));
    }

    @Test
    public void overlappingFirstEventsSerializeAcrossConnectionsAndDisjointWriteCanCommit() throws Exception {
        Event newer = event(TIME, CREATE, "b", "a");
        Event older = event(TIME.minusNanos(1), SOLD, "a", "b");
        PostgresEventRepository first = new PostgresEventRepository(worker("storage-first"));
        PostgresEventRepository second = new PostgresEventRepository(worker("storage-second"));
        PostgresEventRepository unrelated = new PostgresEventRepository(worker("storage-disjoint"));

        ExecutorService threads = Executors.newFixedThreadPool(3);
        try (Gate gate = new Gate("BEFORE", "accepted_event", "NEW.id = '" + newer.getId() + "'::uuid")) {
            Future<?> winner = threads.submit(() -> first.accept(newer));
            awaitBlocked("storage-first");
            Future<?> loser = threads.submit(() -> second.accept(older));
            awaitBlocked("storage-second");

            assertEquals(ACCEPTED, threads.submit(() -> unrelated.accept(event(TIME, CREATE, "c")))
                    .get(5, TimeUnit.SECONDS));
            assertEquals(1, rows("accepted_event"));
            gate.release();
            assertEquals(ACCEPTED, winner.get(5, TimeUnit.SECONDS));
            assertEquals(OUT_OF_ORDER, loser.get(5, TimeUnit.SECONDS));
            assertEquals(2, rows("accepted_event"));
            assertEquals(4, rows("event_outbox"));
            assertEquals(2, query("SELECT count(*) FROM tracked_item WHERE latest_event_id = '" + newer.getId() + "'"));
        } finally {
            threads.shutdownNow();
        }
    }

    @Test
    public void rollbackOnLaterDailyIncrementRemovesKeysAndEarlierDay() throws SQLException {
        long nextDay = TIME.toLocalDate().plusDays(1).toEpochDay();
        execute("CREATE SEQUENCE daily_attempts; "
                + "CREATE FUNCTION count_attempt() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                + "PERFORM nextval('daily_attempts'); RETURN NEW; END $$; "
                + "CREATE TRIGGER observe_day BEFORE INSERT ON daily_statistics "
                + "FOR EACH ROW EXECUTE FUNCTION count_attempt(); "
                + "ALTER TABLE daily_statistics ADD CONSTRAINT reject_day CHECK (epoch_day <> " + nextDay + ")");
        PostgresStatisticsRepository statistics = new PostgresStatisticsRepository(database);
        StatisticsEntry first = new StatisticsEntry(UUID.randomUUID(), TIME.toLocalDate(), 2);
        StatisticsEntry later = new StatisticsEntry(UUID.randomUUID(), TIME.toLocalDate().plusDays(1), 3);

        expect(IllegalStateException.class, () -> statistics.apply(Arrays.asList(later, first)));
        assertEquals(2, query("SELECT last_value FROM daily_attempts"));
        assertEquals(0, rows("statistics_processed"));
        assertEquals(0, rows("daily_statistics"));

        execute("ALTER TABLE daily_statistics DROP CONSTRAINT reject_day");
        statistics.apply(Arrays.asList(first, first, later));
        assertEquals(2, rows("statistics_processed"));
        assertEquals(2, total(first.getDay()));
        assertEquals(3, total(later.getDay()));
    }

    @Test
    public void concurrentConsumersDeduplicateAndAddDistinctEventsToSameDay() throws Exception {
        StatisticsEntry shared = new StatisticsEntry(UUID.randomUUID(), TIME.toLocalDate(), 2);
        StatisticsEntry distinct = new StatisticsEntry(UUID.randomUUID(), TIME.toLocalDate(), 3);
        StatisticsEntry sameDay = new StatisticsEntry(UUID.randomUUID(), TIME.toLocalDate(), 4);
        PostgresStatisticsRepository first = new PostgresStatisticsRepository(worker("statistics-first"));
        PostgresStatisticsRepository second = new PostgresStatisticsRepository(worker("statistics-second"));
        PostgresStatisticsRepository third = new PostgresStatisticsRepository(worker("statistics-third"));

        ExecutorService threads = Executors.newFixedThreadPool(3);
        try (Gate gate = new Gate("AFTER", "daily_statistics", "NEW.item_count = 2")) {
            Future<?> winner = threads.submit(() -> first.apply(Arrays.asList(shared, shared)));
            awaitBlocked("statistics-first");
            Future<?> contender = threads.submit(() -> second.apply(Arrays.asList(distinct, shared)));
            awaitBlocked("statistics-second");
            Future<?> dayContender = threads.submit(() -> third.apply(Collections.singletonList(sameDay)));
            awaitBlocked("statistics-third");

            assertEquals(0, rows("statistics_processed"));
            assertEquals(0, rows("daily_statistics"));
            gate.release();
            winner.get(5, TimeUnit.SECONDS);
            contender.get(5, TimeUnit.SECONDS);
            dayContender.get(5, TimeUnit.SECONDS);

            new PostgresStatisticsRepository(worker("statistics-restarted"))
                    .apply(Arrays.asList(shared, distinct, sameDay));
            assertEquals(3, rows("statistics_processed"));
            assertEquals(9, total(TIME.toLocalDate()));
        } finally {
            threads.shutdownNow();
        }
    }

    private void awaitBlocked(String application) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (System.nanoTime() - deadline < 0) {
            if (query("SELECT count(*) FROM pg_stat_activity WHERE application_name = '" + application(application)
                    + "' AND wait_event_type = 'Lock' AND cardinality(pg_blocking_pids(pid)) > 0") > 0) {
                return;
            }
            Thread.sleep(10);
        }
        fail("No blocked connection observed for " + application);
    }

    private final class Gate implements AutoCloseable {
        private final long key = UUID.randomUUID().getLeastSignificantBits();
        private final Connection connection;

        private Gate(String timing, String table, String predicate) throws SQLException {
            execute("CREATE FUNCTION wait_at_gate() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN "
                    + "PERFORM pg_advisory_xact_lock(" + key + "); RETURN NEW; END $$; "
                    + "CREATE TRIGGER gate " + timing + " INSERT ON " + table
                    + " FOR EACH ROW WHEN (" + predicate + ") EXECUTE FUNCTION wait_at_gate()");
            connection = database.getConnection();
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT pg_advisory_lock(" + key + ")");
            }
        }

        private void release() throws SQLException {
            close();
        }

        @Override
        public void close() throws SQLException {
            connection.close();
        }
    }
}
