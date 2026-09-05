package org.globsframework.sql.drivers.jdbc;

import org.globsframework.sql.RetryPolicy;
import org.globsframework.sql.SqlListener;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.exceptions.ConstraintViolation;
import org.globsframework.sql.exceptions.SerializationFailure;
import org.globsframework.sql.exceptions.UniqueConstraintViolation;
import org.globsframework.sql.model.DummyObject;
import org.junit.Test;

import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class RetryAndListenerTest extends DbServicesTestCase {

    @Test
    public void aDuplicateKeyIsTypedAsSuch() {
        sqlService.runInTransaction(db -> insert(db, 1, "first"));

        UniqueConstraintViolation thrown = assertThrows(UniqueConstraintViolation.class,
                () -> sqlService.runInTransaction(db -> insert(db, 1, "again")));

        // the type it used to be, so existing catch clauses still work
        assertTrue(thrown instanceof ConstraintViolation);
        assertEquals("23505", thrown.getSqlState());
    }

    @Test
    public void aTransientFailureIsReplayed() {
        AtomicInteger attempts = new AtomicInteger();

        String result = sqlService.inTransaction(db -> {
            if (attempts.incrementAndGet() < 3) {
                throw new SerializationFailure(new SQLException("concurrent update", "40001"));
            }
            insert(db, 2, "eventually");
            return "done";
        }, RetryPolicy.onTransientFailures(5, Duration.ofMillis(1)));

        assertEquals("done", result);
        assertEquals(3, attempts.get());
        assertEquals(1, countNamed("eventually"));
    }

    @Test
    public void aRolledBackAttemptLeavesNothingBehind() {
        AtomicInteger attempts = new AtomicInteger();

        sqlService.inTransaction(db -> {
            insert(db, 3 + attempts.get(), "attempt " + attempts.get());
            if (attempts.incrementAndGet() < 2) {
                throw new SerializationFailure(new SQLException("concurrent update", "40001"));
            }
            return null;
        }, RetryPolicy.onTransientFailures(3, Duration.ofMillis(1)));

        // the row written by the attempt that failed was rolled back with its transaction
        assertEquals(0, countNamed("attempt 0"));
        assertEquals(1, countNamed("attempt 1"));
    }

    @Test
    public void whatIsNotTransientIsNotReplayed() {
        AtomicInteger attempts = new AtomicInteger();
        sqlService.runInTransaction(db -> insert(db, 4, "taken"));

        assertThrows(UniqueConstraintViolation.class, () -> sqlService.inTransaction(db -> {
            attempts.incrementAndGet();
            insert(db, 4, "taken again");
            return null;
        }, RetryPolicy.onTransientFailures(5, Duration.ofMillis(1))));

        assertEquals(1, attempts.get());
    }

    @Test
    public void givingUpRethrowsTheLastFailure() {
        AtomicInteger attempts = new AtomicInteger();

        assertThrows(SerializationFailure.class, () -> sqlService.inTransaction(db -> {
            attempts.incrementAndGet();
            throw new SerializationFailure(new SQLException("concurrent update", "40001"));
        }, RetryPolicy.onTransientFailures(3, Duration.ofMillis(1))));

        assertEquals(3, attempts.get());
    }

    @Test
    public void noRetryPolicyIsTheDefault() {
        assertEquals(Optional.empty(),
                sqlService.getRetryPolicy().nextDelay(new SerializationFailure(new SQLException("x", "40001")), 1));
    }

    @Test
    public void theListenerSeesEveryStatement() {
        List<String> statements = new ArrayList<>();
        List<Integer> rowCounts = new ArrayList<>();
        sqlService.setListener((sql, durationNanos, rowCount, error) -> {
            statements.add(sql);
            rowCounts.add(rowCount);
            assertTrue("duration should be measured", durationNanos > 0);
            assertNull(error);
        });

        sqlConnection.getCreateBuilder(DummyObject.TYPE)
                .set(DummyObject.ID, 5).set(DummyObject.NAME, "watched").getRequest().apply();
        sqlConnection.getUpdateBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 5))
                .update(DummyObject.NAME, "renamed").getRequest().apply();
        sqlConnection.getQueryBuilder(DummyObject.TYPE).selectAll().getQuery().executeAsGlobs();
        sqlConnection.getDeleteRequest(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 5)).apply();

        assertEquals(statements.toString(), 4, statements.size());
        assertTrue(statements.get(0), statements.get(0).startsWith("INSERT INTO "));
        assertTrue(statements.get(1), statements.get(1).startsWith("UPDATE "));
        assertTrue(statements.get(2), statements.get(2).startsWith("select "));
        assertTrue(statements.get(3), statements.get(3).startsWith("DELETE "));
        // one row inserted, one updated, unknown for the select, one deleted
        assertEquals(List.of(1, 1, -1, 1), rowCounts);
    }

    @Test
    public void theListenerSeesFailures() {
        sqlService.runInTransaction(db -> insert(db, 6, "first"));
        List<Throwable> errors = new ArrayList<>();
        sqlService.setListener((sql, durationNanos, rowCount, error) -> {
            if (error != null) {
                errors.add(error);
            }
        });

        assertThrows(UniqueConstraintViolation.class,
                () -> sqlService.runInTransaction(db -> insert(db, 6, "again")));

        assertEquals(1, errors.size());
        assertTrue(errors.get(0) instanceof UniqueConstraintViolation);
    }

    private void insert(org.globsframework.sql.SqlConnection db, int id, String name) {
        db.getCreateBuilder(DummyObject.TYPE).set(DummyObject.ID, id).set(DummyObject.NAME, name)
                .getRequest().apply();
    }

    private int countNamed(String name) {
        return sqlService.read(db -> db.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.NAME, name))
                .selectAll().getQuery().executeAsGlobs().size());
    }
}
