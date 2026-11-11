package com.persequor.repository.postgres;

import com.persequor.repository.OutboxRepository;

import javax.sql.DataSource;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

public final class PostgresOutboxRepository implements OutboxRepository {
    private final JdbcTransactions transactions;

    public PostgresOutboxRepository(DataSource dataSource) {
        transactions = new JdbcTransactions(dataSource);
    }

    @Override
    public List<Intent> due(Instant now, int limit) {
        return transactions.commit("Read due intents", connection -> {
            try (PreparedStatement select = connection.prepareStatement(
                    "SELECT e.payload, o.destination, o.failure_count FROM event_outbox o "
                            + "JOIN accepted_event e ON e.id = o.event_id "
                            + "WHERE o.confirmed_at IS NULL AND o.next_attempt_at <= ? "
                            + "ORDER BY o.next_attempt_at, o.event_id, o.destination LIMIT ?")) {
                select.setObject(1, now.atOffset(ZoneOffset.UTC));
                select.setInt(2, limit);
                try (ResultSet rows = select.executeQuery()) {
                    List<Intent> intents = new ArrayList<>();
                    while (rows.next()) {
                        intents.add(new Intent(EventPayload.decode(rows.getString(1)), rows.getString(2), rows.getInt(3)));
                    }
                    return intents;
                }
            }
        });
    }

    @Override
    public void confirm(Intent intent, Instant confirmedAt) {
        transactions.commit("Record broker confirmation", connection -> {
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE event_outbox SET confirmed_at = ?, last_error = NULL "
                            + "WHERE event_id = ? AND destination = ? AND confirmed_at IS NULL")) {
                update.setObject(1, confirmedAt.atOffset(ZoneOffset.UTC));
                update.setObject(2, intent.getEvent().getId());
                update.setString(3, intent.getDestination());
                requireOne(update);
            }
            return null;
        });
    }

    @Override
    public void retry(Intent intent, Instant nextAttempt, String error) {
        transactions.commit("Record outbox retry", connection -> {
            try (PreparedStatement update = connection.prepareStatement(
                    "UPDATE event_outbox SET next_attempt_at = ?, failure_count = failure_count + 1, last_error = ? "
                            + "WHERE event_id = ? AND destination = ? AND confirmed_at IS NULL")) {
                update.setObject(1, nextAttempt.atOffset(ZoneOffset.UTC));
                update.setString(2, error);
                update.setObject(3, intent.getEvent().getId());
                update.setString(4, intent.getDestination());
                requireOne(update);
            }
            return null;
        });
    }

    private static void requireOne(PreparedStatement update) throws SQLException {
        if (update.executeUpdate() != 1) {
            throw new SQLException("Expected one pending outbox intent");
        }
    }
}
