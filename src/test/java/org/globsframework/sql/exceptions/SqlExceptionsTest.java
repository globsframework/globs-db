package org.globsframework.sql.exceptions;

import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

/**
 * Only 23000 and 23505 used to be recognised; everything else became an opaque SqlException, which
 * left a caller no way to tell a duplicate key from a deadlock and so no way to decide whether
 * retrying made sense.
 */
public class SqlExceptionsTest {

    @Test
    public void standardStatesAreClassified() {
        assertTyped(UniqueConstraintViolation.class, "23505", 0);
        assertTyped(ForeignKeyViolation.class, "23503", 0);
        assertTyped(NotNullViolation.class, "23502", 0);
        assertTyped(CheckConstraintViolation.class, "23514", 0);
        assertTyped(SerializationFailure.class, "40001", 0);
        assertTyped(DeadlockDetected.class, "40P01", 0);
        assertTyped(LockTimeout.class, "55P03", 0);
        assertTyped(QueryCanceled.class, "57014", 0);
    }

    @Test
    public void anyConnectionStateIsAConnectionFailure() {
        assertTyped(ConnectionFailed.class, "08000", 0);
        assertTyped(ConnectionFailed.class, "08006", 0);
        assertTyped(ConnectionFailed.class, "08S01", 0);
    }

    @Test
    public void mysqlLumpsEveryIntegrityViolationUnder23000() {
        assertTyped(UniqueConstraintViolation.class, "23000", 1062);
        assertTyped(ForeignKeyViolation.class, "23000", 1452);
        assertTyped(NotNullViolation.class, "23000", 1048);
        // an integrity violation whose vendor code says nothing stays a plain ConstraintViolation
        assertTyped(ConstraintViolation.class, "23000", 0);
    }

    @Test
    public void oracleCodesAreRecognisedToo() {
        assertTyped(UniqueConstraintViolation.class, "23000", 1);
        assertTyped(ForeignKeyViolation.class, "23000", 2291);
        assertTyped(DeadlockDetected.class, "61000", 60);
    }

    @Test
    public void mysqlDeadlockAndLockTimeout() {
        assertTyped(DeadlockDetected.class, "40001", 1213);
        assertTyped(LockTimeout.class, "HY000", 1205);
    }

    @Test
    public void whatIsRetryableIsTransient() {
        assertTrue(typed("40001", 0) instanceof TransientSqlException);
        assertTrue(typed("40P01", 0) instanceof TransientSqlException);
        assertTrue(typed("55P03", 0) instanceof TransientSqlException);
        assertTrue(typed("08006", 0) instanceof TransientSqlException);

        // the data is at fault: a retry would fail identically
        assertTrue(typed("23505", 0) instanceof ConstraintViolation);
        assertTrue(!(typed("23505", 0) instanceof TransientSqlException));
        // a statement already too slow is not worth replaying either
        assertTrue(!(typed("57014", 0) instanceof TransientSqlException));
    }

    @Test
    public void anUnknownFailureStaysAPlainSqlException() {
        assertEquals(SqlException.class, typed("XX999", 0).getClass());
        assertEquals(SqlException.class, typed(null, 0).getClass());
    }

    @Test
    public void theSqlStateAndErrorCodeStayReadable() {
        SQLException cause = new SQLException("duplicate", "23505", 42);
        SqlException typed = SqlExceptions.typed("insert into x", cause);

        assertEquals("23505", typed.getSqlState());
        assertEquals(42, typed.getErrorCode());
        assertSame(cause, typed.getCause());
        assertTrue(typed.getMessage(), typed.getMessage().contains("insert into x"));
    }

    private void assertTyped(Class<? extends SqlException> expected, String sqlState, int errorCode) {
        assertEquals(sqlState + "/" + errorCode, expected, typed(sqlState, errorCode).getClass());
    }

    private SqlException typed(String sqlState, int errorCode) {
        return SqlExceptions.typed(null, new SQLException("boom", sqlState, errorCode));
    }
}
