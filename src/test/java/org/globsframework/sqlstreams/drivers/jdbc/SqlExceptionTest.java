package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.metamodel.DummyObject;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class SqlExceptionTest {

    public abstract SqlConnection getDb();

    @Before
    public void setUp() throws Exception {
        SqlConnection sqlConnection = getDb();
        sqlConnection.createTable(DummyObject.TYPE);
        sqlConnection.getDeleteRequest(DummyObject.TYPE).run();
        sqlConnection.commitAndClose();
    }

    @Test
    public void testRollback() throws Exception {
        SqlConnection db1 = getDb();
        db1.getCreateBuilder(DummyObject.TYPE).set(DummyObject.ID, 1).set(DummyObject.NAME, "toto").getRequest().run();
        db1.commit();
        SelectQuery query1 = db1.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .select(DummyObject.NAME).getQuery();
        Assert.assertEquals("toto", query1.executeUnique().get(DummyObject.NAME));
        db1.getUpdateBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .update(DummyObject.NAME, "titi").getRequest().run();
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
        db1.getCreateBuilder(DummyObject.TYPE).set(DummyObject.ID, 1).set(DummyObject.NAME, "toto").getRequest().run();
        db1.commit();
        Glob glob1 = db1.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .select(DummyObject.NAME).getQuery().executeUnique();
        Assert.assertEquals("toto", glob1.get(DummyObject.NAME));
        db1.getUpdateBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .update(DummyObject.NAME, "titi").getRequest().run();

        SelectQuery query2 = db2.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .select(DummyObject.NAME).getQuery();
        Glob glob2 = query2.executeUnique();
        Assert.assertEquals("titi", glob2.get(DummyObject.NAME));
        db2.getUpdateBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                .update(DummyObject.NAME, "tata").getRequest().run();
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