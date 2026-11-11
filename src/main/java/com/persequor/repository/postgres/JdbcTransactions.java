package com.persequor.repository.postgres;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

final class JdbcTransactions {
    private final DataSource dataSource;

    JdbcTransactions(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    <T> T commit(String operation, Work<T> work) {
        try (Connection connection = dataSource.getConnection()) {
            connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            connection.setAutoCommit(false);
            try {
                T result = work.run(connection);
                connection.commit();
                return result;
            } catch (SQLException | RuntimeException e) {
                try {
                    connection.rollback();
                } catch (SQLException rollbackFailure) {
                    e.addSuppressed(rollbackFailure);
                }
                throw e;
            }
        } catch (SQLException e) {
            throw new IllegalStateException(operation + " failed (SQLState " + e.getSQLState() + ")", e);
        }
    }

    interface Work<T> {
        T run(Connection connection) throws SQLException;
    }
}
