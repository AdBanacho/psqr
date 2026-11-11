package com.persequor.repository.postgres;

import com.persequor.model.StatisticsEntry;
import com.persequor.repository.StatisticsRepository;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public final class PostgresStatisticsRepository implements StatisticsRepository {
    private final JdbcTransactions transactions;

    public PostgresStatisticsRepository(DataSource dataSource) {
        transactions = new JdbcTransactions(dataSource);
    }

    @Override
    public void apply(List<StatisticsEntry> entries) {
        transactions.commit("Apply statistics batch", connection -> {
            incrementDays(connection, StatisticsEntry.totals(insertNewKeys(connection, entries)));
            return null;
        });
    }

    private List<StatisticsEntry> insertNewKeys(Connection connection, List<StatisticsEntry> entries)
            throws SQLException {
        Map<UUID, StatisticsEntry> distinct = new TreeMap<>();
        for (StatisticsEntry entry : entries) {
            distinct.putIfAbsent(entry.getId(), entry);
        }

        List<StatisticsEntry> inserted = new ArrayList<>();
        try (PreparedStatement insert = connection.prepareStatement(
                "INSERT INTO statistics_processed (event_id) VALUES (?) ON CONFLICT DO NOTHING")) {
            for (StatisticsEntry entry : distinct.values()) {
                insert.setObject(1, entry.getId());
                if (insert.executeUpdate() == 1) {
                    inserted.add(entry);
                }
            }
        }
        return inserted;
    }

    private void incrementDays(Connection connection, Map<LocalDate, Long> totals) throws SQLException {
        try (PreparedStatement update = connection.prepareStatement(
                "INSERT INTO daily_statistics (epoch_day, item_count) VALUES (?, ?) ON CONFLICT (epoch_day) DO UPDATE "
                        + "SET item_count = daily_statistics.item_count + EXCLUDED.item_count")) {
            for (Map.Entry<LocalDate, Long> total : totals.entrySet()) {
                update.setLong(1, total.getKey().toEpochDay());
                update.setLong(2, total.getValue());
                update.executeUpdate();
            }
        }
    }
}
