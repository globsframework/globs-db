package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.utils.exceptions.UnexpectedApplicationState;
import org.globsframework.sql.SqlConnection;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.drivers.jdbc.pool.ConnectionPools;
import org.globsframework.sql.drivers.jdbc.pool.PoolConfig;
import org.globsframework.sql.exceptions.SqlException;
import org.globsframework.sql.model.DummyObject;
import org.junit.Assume;
import org.junit.Test;

import java.io.IOException;
import java.time.Duration;

import static org.junit.Assert.*;

public class SqlServiceTransactionTest extends DbServicesTestCase {

    @Test
    public void closeRollsBackUncommittedWork() {
        SqlConnection connection = sqlService.getDb();
        insert(connection, "uncommitted");
        connection.close();

        assertEquals(0, count("uncommitted"));
    }

    @Test
    public void closeKeepsCommittedWork() {
        SqlConnection connection = sqlService.getDb();
        insert(connection, "committed");
        connection.commit();
        connection.close();

        assertEquals(1, count("committed"));
    }

    @Test
    public void closeIsIdempotent() {
        SqlConnection connection = sqlService.getDb();
        connection.commitAndClose();
        connection.close();
        connection.close();
    }

    @Test
    public void tryWithResourcesRollsBackOnException() {
        try (SqlConnection connection = sqlService.getDb()) {
            insert(connection, "aborted");
            throw new IllegalStateException("boom");
        } catch (IllegalStateException e) {
            assertEquals("boom", e.getMessage());
        }
        assertEquals(0, count("aborted"));
    }

    @Test
    public void inTransactionCommitsAndReturnsTheResult() {
        String name = sqlService.inTransaction(connection -> {
            insert(connection, "in transaction");
            return "done";
        });

        assertEquals("done", name);
        assertEquals(1, count("in transaction"));
    }

    @Test
    public void inTransactionRollsBackAndRethrowsUnchanged() {
        IllegalStateException thrown = assertThrows(IllegalStateException.class,
                () -> sqlService.inTransaction(connection -> {
                    insert(connection, "rolled back");
                    throw new IllegalStateException("boom");
                }));

        assertEquals("boom", thrown.getMessage());
        assertEquals(0, count("rolled back"));
    }

    @Test
    public void inTransactionWrapsCheckedExceptions() {
        SqlException thrown = assertThrows(SqlException.class,
                () -> sqlService.inTransaction(connection -> {
                    throw new IOException("no disk");
                }));

        assertEquals("no disk", thrown.getMessage());
        assertEquals(IOException.class, thrown.getCause().getClass());
    }

    @Test
    public void runInTransactionCommits() {
        sqlService.runInTransaction(connection -> insert(connection, "run in transaction"));

        assertEquals(1, count("run in transaction"));
    }

    @Test
    public void readRunsOnAnAutoCommitConnection() {
        sqlService.runInTransaction(connection -> insert(connection, "readable"));

        long found = sqlService.read(connection -> connection
                .getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.NAME, "readable"))
                .selectAll()
                .getQuery()
                .executeAsGlobs()
                .size());

        assertEquals(1, found);
    }

    @Test
    public void connectionsAreReturnedToThePool() {
        // a pool of two: without close() handing connections back, the third iteration would block
        // until connectionTimeout and fail
        PoolConfig config = PoolConfig.DEFAULT
                .withMaxPoolSize(2)
                .withMinIdle(1)
                .withConnectionTimeout(Duration.ofSeconds(2))
                .withPoolName("returned-to-pool");
        try (JdbcSqlService service = new JdbcSqlService("jdbc:hsqldb:.", "sa", "", config)) {
            Assume.assumeTrue(service.isPooled());
            for (int i = 0; i < 20; i++) {
                final int index = i;
                service.runInTransaction(connection -> insert(connection, "pooled " + index));
            }
            assertEquals(20, countStartingWith("pooled "));
        }
    }

    @Test
    public void noPoolConfigFallsBackToDirectConnections() {
        try (JdbcSqlService service = new JdbcSqlService("jdbc:hsqldb:.", "sa", "", PoolConfig.NO_POOL)) {
            service.runInTransaction(connection -> insert(connection, "unpooled"));
            assertEquals(1, count("unpooled"));
        }
    }

    @Test
    public void aClosedServiceHandsOutNoMoreConnections() {
        JdbcSqlService service = new JdbcSqlService("jdbc:hsqldb:.", "sa", "");
        service.close();
        service.close();

        assertThrows(UnexpectedApplicationState.class, service::getDb);
    }

    @Test
    public void hikariIsOnTheTestClasspath() {
        // guards the assumption made by connectionsAreReturnedToThePool
        assertTrue(ConnectionPools.isAvailable());
    }

    private void insert(SqlConnection connection, String name) {
        connection.getCreateBuilder(DummyObject.TYPE)
                .set(DummyObject.NAME, name)
                .getRequest()
                .apply();
    }

    private int count(String name) {
        return count(Constraints.equal(DummyObject.NAME, name));
    }

    private int countStartingWith(String prefix) {
        return count(Constraints.startWith(DummyObject.NAME, prefix));
    }

    private int count(Constraint constraint) {
        return sqlService.read(connection -> connection
                .getQueryBuilder(DummyObject.TYPE, constraint)
                .selectAll()
                .getQuery()
                .executeAsGlobs()
                .size());
    }
}
