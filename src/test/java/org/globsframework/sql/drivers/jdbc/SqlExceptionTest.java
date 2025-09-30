package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.model.Glob;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.SqlConnection;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.model.DummyObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class SqlExceptionTest {

    public abstract SqlConnection getDb();

    @Before
    public void setUp() throws Exception {
        SqlConnection sqlConnection = getDb();
        sqlConnection.createTable(DummyObject.TYPE);
        sqlConnection.getDeleteRequest(DummyObject.TYPE).apply();
        sqlConnection.commitAndClose();
    }

    @Test
    public void testRollback() throws Exception {
        SqlConnection db1 = getDb();
        db1.getCreateBuilder(DummyObject.TYPE).set(DummyObject.ID, 1).set(DummyObject.NAME, "toto").getRequest().apply();
        db1.commit();
        SelectQuery query1 = db1.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .select(DummyObject.NAME).getQuery();
        Assert.assertEquals("toto", query1.executeUnique().get(DummyObject.NAME));
        db1.getUpdateBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .update(DummyObject.NAME, "titi").getRequest().apply();
        Assert.assertEquals("titi", query1.executeUnique().get(DummyObject.NAME));
        db1.rollbackAndClose();
        SqlConnection db2 = getDb();
        Assert.assertEquals("toto", db2.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .select(DummyObject.NAME).getQuery().executeUnique().get(DummyObject.NAME));
    }

    @Test
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
                .select(DummyObject.NAME).getQuery();
        Glob glob2 = query2.executeUnique();
        Assert.assertEquals("titi", glob2.get(DummyObject.NAME));
        db2.getUpdateBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .update(DummyObject.NAME, "tata").getRequest().apply();
//    db1.rollbackAndClose();
        glob2 = query2.executeUnique();
        Assert.assertEquals("tata", glob2.get(DummyObject.NAME));
        db2.commit();
        db1.rollbackAndClose();
        Glob newGlob = getDb().getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .select(DummyObject.NAME).getQuery().executeUnique();
        Assert.assertEquals("toto", newGlob.get(DummyObject.NAME));
    }

}
