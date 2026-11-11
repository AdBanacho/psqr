package com.persequor.broker;

import org.junit.After;
import org.junit.Before;
import org.postgresql.ds.PGSimpleDataSource;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.Scanner;
import java.util.UUID;

import static org.junit.Assert.assertNotNull;

public abstract class PostgresFixture {
    private String schema;
    private PGSimpleDataSource admin;
    PGSimpleDataSource database;

    @Before
    public void migrateSchema() throws SQLException {
        String url = System.getenv("PSQR_JDBC_URL");
        assertNotNull("Set PSQR_JDBC_URL to a PostgreSQL database for this test", url);
        admin = dataSource(url);
        schema = "psqr_" + UUID.randomUUID().toString().replace("-", "");
        try (Connection connection = admin.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("CREATE SCHEMA " + schema);
        }
        database = worker("psqr-test");
        InputStream migration = getClass().getResourceAsStream("/db/migration/V001__event_delivery.sql");
        assertNotNull(migration);
        try (Scanner sql = new Scanner(migration, "UTF-8").useDelimiter("\\A")) {
            execute(sql.next());
        }
    }

    @After
    public void dropSchema() throws SQLException {
        if (schema != null) {
            try (Connection connection = admin.getConnection(); Statement statement = connection.createStatement()) {
                statement.execute("DROP SCHEMA IF EXISTS " + schema + " CASCADE");
            }
        }
    }

    PGSimpleDataSource worker(String applicationName) {
        PGSimpleDataSource dataSource = dataSource(System.getenv("PSQR_JDBC_URL"));
        dataSource.setCurrentSchema(schema);
        dataSource.setApplicationName(application(applicationName));
        return dataSource;
    }

    String application(String name) {
        return name + "-" + schema.substring(5, 13);
    }

    private PGSimpleDataSource dataSource(String url) {
        PGSimpleDataSource dataSource = new PGSimpleDataSource();
        dataSource.setURL(url);
        dataSource.setUser(System.getenv().getOrDefault("PSQR_JDBC_USER", "postgres"));
        dataSource.setPassword(System.getenv().getOrDefault("PSQR_JDBC_PASSWORD", ""));
        return dataSource;
    }

    void execute(String sql) throws SQLException {
        try (Connection connection = database.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute(sql);
        }
    }

    long query(String sql) {
        try (Connection connection = database.getConnection();
             Statement statement = connection.createStatement();
             ResultSet rows = statement.executeQuery(sql)) {
            if (!rows.next()) {
                throw new AssertionError("No row for " + sql);
            }
            return rows.getLong(1);
        } catch (SQLException e) {
            throw new AssertionError(sql, e);
        }
    }

    long rows(String table) {
        return query("SELECT count(*) FROM " + table);
    }

    long total(LocalDate day) {
        return query("SELECT coalesce(sum(item_count), 0) FROM daily_statistics WHERE epoch_day = " + day.toEpochDay());
    }
}
