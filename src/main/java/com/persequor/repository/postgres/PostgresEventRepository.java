package com.persequor.repository.postgres;

import com.persequor.model.Event;
import com.persequor.model.EventOrdering;
import com.persequor.repository.EventRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

public final class PostgresEventRepository implements EventRepository {
    private final JdbcTransactions transactions;

    public PostgresEventRepository(DataSource dataSource) {
        transactions = new JdbcTransactions(dataSource);
    }

    @Override
    public Decision accept(Event event) {
        return transactions.commit("Accept event", connection -> accept(connection, event));
    }

    private Decision accept(Connection connection, Event event) throws SQLException {
        // The two-integer advisory key space is reserved for UUID arbitration in this schema.
        // A hash collision can add contention. It cannot make two UUIDs compare equal.
        try (PreparedStatement lock = connection.prepareStatement("SELECT pg_advisory_xact_lock(?, ?)")) {
            lock.setInt(1, 173918);
            lock.setInt(2, event.getId().hashCode());
            lock.execute();
        }
        Event previous = loadEvent(connection, event.getId());
        if (previous != null) {
            return EventOrdering.sameContent(event, previous) ? Decision.REPLAY : Decision.UUID_CONFLICT;
        }

        Set<String> items = new TreeSet<>(event.getTrackedItemIds());
        Set<UUID> latestIds = lockItems(connection, items);
        for (UUID latestId : latestIds) {
            if (EventOrdering.precedes(event, loadEvent(connection, latestId))) {
                // The exception path is reserved for infrastructure failures. Sentinel rows
                // created for new IDs by a rejection are safe to retain with null heads.
                return Decision.OUT_OF_ORDER;
            }
        }

        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO accepted_event (id, payload) VALUES (?, ?)")) {
            insert.setObject(1, event.getId());
            insert.setString(2, EventPayload.encode(event));
            insert.executeUpdate();
        }
        try (PreparedStatement update = connection.prepareStatement(
                "UPDATE tracked_item SET latest_event_id = ? WHERE item_id = ?")) {
            for (String item : items) {
                update.setObject(1, event.getId());
                update.setString(2, item);
                update.addBatch();
            }
            update.executeBatch();
        }
        try (PreparedStatement outbox = connection.prepareStatement(
                "INSERT INTO event_outbox (event_id, destination) VALUES (?, 'statistics'), (?, 'subscription')")) {
            outbox.setObject(1, event.getId());
            outbox.setObject(2, event.getId());
            outbox.executeUpdate();
        }
        return Decision.ACCEPTED;
    }

    private Set<UUID> lockItems(Connection connection, Set<String> items) throws SQLException {
        Set<UUID> latestIds = new HashSet<>();
        try (PreparedStatement ensure = connection.prepareStatement(
                     "INSERT INTO tracked_item (item_id) VALUES (?) ON CONFLICT DO NOTHING");
             PreparedStatement lock = connection.prepareStatement(
                     "SELECT latest_event_id FROM tracked_item WHERE item_id = ? FOR UPDATE")) {
            for (String item : items) {
                ensure.setString(1, item);
                ensure.executeUpdate();
                lock.setString(1, item);
                try (ResultSet row = lock.executeQuery()) {
                    if (!row.next()) {
                        throw new SQLException("Missing tracked-item lock row");
                    }
                    UUID latestId = row.getObject(1, UUID.class);
                    if (latestId != null) {
                        latestIds.add(latestId);
                    }
                }
            }
        }
        return latestIds;
    }

    private Event loadEvent(Connection connection, UUID id) throws SQLException {
        try (PreparedStatement select = connection.prepareStatement("SELECT payload FROM accepted_event WHERE id = ?")) {
            select.setObject(1, id);
            try (ResultSet row = select.executeQuery()) {
                return row.next() ? EventPayload.decode(row.getString(1)) : null;
            }
        }
    }
}
