package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.model.Glob;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.SqlConnection;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.drivers.jdbc.pool.PoolConfig;
import org.globsframework.sql.model.DummyObject;
import org.globsframework.sql.testdb.TestDb;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

/**
 * Transaction semantics, on whatever backend the suite runs against. This used to be an abstract
 * class with one subclass per dialect, each hardcoding a url, and all of them abstract too — so none
 * of it ever ran. TestDb provides the connection now.
 */
public class SqlExceptionTest {
    private JdbcSqlService sqlService;

    public SqlConnection getDb() {
        if (sqlService == null) {
            // several connections are held at once here, the pool must be able to serve them
            sqlService = TestDb.createService(PoolConfig.DEFAULT.withMaxPoolSize(4));
        }
        return sqlService.getDb();
    }

    @Before
    public void setUp() throws Exception {
        SqlConnection sqlConnection = getDb();
        sqlConnection.createTable(DummyObject.TYPE);
        sqlConnection.getDeleteRequest(DummyObject.TYPE).apply();
        sqlConnection.commitAndClose();
    }

    @After
    public void tearDown() {
        if (sqlService != null) {
            sqlService.close();
            sqlService = null;
        }
    }

    @Test(timeout = 30_000)
    public void testRollback() throws Exception {
        SqlConnection db1 = getDb();
        db1.getCreateBuilder(DummyObject.TYPE).set(DummyObject.ID, 1).set(DummyObject.NAME, "toto").getRequest().apply();
        db1.commit();

        // re-executed twice, so it must not close itself after the first stream is exhausted
        try (SelectQuery query1 = db1.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .select(DummyObject.NAME).getNotAutoCloseQuery()) {
            Assert.assertEquals("toto", query1.executeUnique().get(DummyObject.NAME));
            db1.getUpdateBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                    .update(DummyObject.NAME, "titi").getRequest().apply();
            Assert.assertEquals("titi", query1.executeUnique().get(DummyObject.NAME));
        }
        db1.rollbackAndClose();

        try (SqlConnection db2 = getDb()) {
            Glob afterRollback = db2.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                    .select(DummyObject.NAME).getQuery().executeUnique();
            Assert.assertEquals("toto", afterRollback.get(DummyObject.NAME));
        }
    }

    @Test(timeout = 30_000)
    @Ignore("Asserts a dirty read: db2 is expected to see a row db1 has updated but not committed. " +
            "No backend gives that at its default isolation — HSQLDB blocks on the write lock until " +
            "the timeout, PostgreSQL would too. Rewrite it around an isolation level before enabling.")
    public void testConcurrentModification() throws Exception {
        SqlConnection db1 = getDb();
        SqlConnection db2 = getDb();
        db1.getCreateBuilder(DummyObject.TYPE).set(DummyObject.ID, 1).set(DummyObject.NAME, "toto").getRequest().apply();
        db1.commit();
        Glob glob1 = db1.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .select(DummyObject.NAME).getQuery().executeUnique();
        Assert.assertEquals("toto", glob1.get(DummyObject.NAME));
        db1.getUpdateBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .update(DummyObject.NAME, "titi").getRequest().apply();

        SelectQuery query2 = db2.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .select(DummyObject.NAME).getNotAutoCloseQuery();
        Glob glob2 = query2.executeUnique();
        Assert.assertEquals("titi", glob2.get(DummyObject.NAME));
        db2.getUpdateBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .update(DummyObject.NAME, "tata").getRequest().apply();
        glob2 = query2.executeUnique();
        Assert.assertEquals("tata", glob2.get(DummyObject.NAME));
        query2.close();
        db2.commit();
        db1.rollbackAndClose();

        try (SqlConnection db3 = getDb()) {
            Glob newGlob = db3.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                    .select(DummyObject.NAME).getQuery().executeUnique();
            Assert.assertEquals("toto", newGlob.get(DummyObject.NAME));
        }
        db2.close();
    }
}
