package org.globsframework.sql.exceptions;

import java.sql.SQLException;

/**
 * Turns a driver's {@link SQLException} into the exception this library throws.
 * <p>
 * The classification is driven by the SQLState, which is standard where it matters, and refined by
 * the vendor error code where a database lumps distinct failures under one state — MySQL and Oracle
 * both report every integrity violation as 23000. Anything unrecognised stays a plain
 * {@link SqlException}, so an unknown state degrades to the previous behaviour rather than being
 * mislabelled.
 * <p>
 * What this buys, beyond a readable catch clause, is that the caller can tell a failure caused by
 * the data — a {@link ConstraintViolation}, which will fail again identically — from one caused by
 * concurrency or infrastructure — a {@link TransientSqlException}, which a retry may well clear.
 */
public class SqlExceptions {

    private SqlExceptions() {
    }

    public static SqlException typed(String sql, SQLException e) {
        return switch (kindOf(e)) {
            case UNIQUE -> sql == null ? new UniqueConstraintViolation(e) : new UniqueConstraintViolation(sql, e);
            case FOREIGN_KEY -> sql == null ? new ForeignKeyViolation(e) : new ForeignKeyViolation(sql, e);
            case NOT_NULL -> sql == null ? new NotNullViolation(e) : new NotNullViolation(sql, e);
            case CHECK -> sql == null ? new CheckConstraintViolation(e) : new CheckConstraintViolation(sql, e);
            case CONSTRAINT -> sql == null ? new ConstraintViolation(e) : new ConstraintViolation(sql, e);
            case SERIALIZATION -> sql == null ? new SerializationFailure(e) : new SerializationFailure(sql, e);
            case DEADLOCK -> sql == null ? new DeadlockDetected(e) : new DeadlockDetected(sql, e);
            case LOCK_TIMEOUT -> sql == null ? new LockTimeout(e) : new LockTimeout(sql, e);
            case CONNECTION -> sql == null ? new ConnectionFailed(e) : new ConnectionFailed(sql, e);
            case CANCELED -> sql == null ? new QueryCanceled(e) : new QueryCanceled(sql, e);
            case UNKNOWN -> sql == null ? new SqlException(e) : new SqlException(sql, e);
        };
    }

    enum Kind {
        UNIQUE, FOREIGN_KEY, NOT_NULL, CHECK, CONSTRAINT,
        SERIALIZATION, DEADLOCK, LOCK_TIMEOUT, CONNECTION,
        CANCELED, UNKNOWN
    }

    static Kind kindOf(SQLException e) {
        String state = e.getSQLState();
        if (state == null) {
            return byVendorCode(e, Kind.UNKNOWN);
        }
        return switch (state) {
            case "23505", "21000" -> Kind.UNIQUE;
            case "23503", "23001" -> Kind.FOREIGN_KEY;
            case "23502" -> Kind.NOT_NULL;
            case "23514" -> Kind.CHECK;

            // MySQL reports a deadlock as 40001 too, only the vendor code separates the two
            case "40001" -> byVendorCode(e, Kind.SERIALIZATION);
            case "40P01", "61000" -> Kind.DEADLOCK;
            case "55P03" -> Kind.LOCK_TIMEOUT;
            case "53300", "53400" -> Kind.CONNECTION;

            case "57014", "72000" -> Kind.CANCELED;

            // 23000 and HY000 are catch-alls: only the vendor code says what actually happened
            default -> state.startsWith("08") ? Kind.CONNECTION
                    : state.startsWith("23") ? byVendorCode(e, Kind.CONSTRAINT)
                    : byVendorCode(e, Kind.UNKNOWN);
        };
    }

    private static Kind byVendorCode(SQLException e, Kind fallback) {
        return switch (e.getErrorCode()) {
            // MySQL and MariaDB
            case 1062, 1586 -> Kind.UNIQUE;
            case 1451, 1452 -> Kind.FOREIGN_KEY;
            case 1048, 1364 -> Kind.NOT_NULL;
            case 3819 -> Kind.CHECK;
            case 1213 -> Kind.DEADLOCK;
            case 1205 -> Kind.LOCK_TIMEOUT;
            case 1317 -> Kind.CANCELED;

            // Oracle
            case 1 -> Kind.UNIQUE;
            case 2291, 2292 -> Kind.FOREIGN_KEY;
            case 1400, 1407 -> Kind.NOT_NULL;
            case 2290 -> Kind.CHECK;
            case 60 -> Kind.DEADLOCK;
            case 1013 -> Kind.CANCELED;

            default -> fallback;
        };
    }
}
