package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.directory.DefaultDirectory;
import org.globsframework.directory.Directory;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobModel;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.impl.DefaultGlobModel;
import org.globsframework.model.DummyObject;
import org.globsframework.model.DummyObject2;
import org.globsframework.model.Key;
import org.globsframework.sqlstreams.*;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.globsframework.sqlstreams.utils.MultiCreateBuilder;
import org.globsframework.streams.GlobStream;
import org.globsframework.streams.accessors.Accessor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class DbServicesTestCase {
    protected JdbcSqlService sqlService;
    protected SqlConnection sqlConnection;
    protected DefaultGlobModel globModel;
    protected Directory directory = new DefaultDirectory();

    @Before
    public void setUp() throws Exception {
        globModel = new DefaultGlobModel(DummyObject.TYPE, DummyObject2.TYPE);
        sqlConnection = initDb();
        sqlConnection.createTable(DummyObject.TYPE);
        sqlConnection.createTable(DummyObject2.TYPE);
        emptyTable();
    }

    @After
    public void tearDown() throws Exception {
        emptyTable();
        sqlConnection = null;
    }

    private void emptyTable() {
        sqlConnection.emptyTable(DummyObject.TYPE);
        sqlConnection.emptyTable(DummyObject2.TYPE);
        sqlConnection.commit();
    }

    private SqlConnection initDb() {

        directory.add(GlobModel.class, globModel);

        sqlService = new JdbcSqlService("jdbc:hsqldb:.", "sa", "");
//    sqlService = new JdbcDriverBasedSqlService("jdbc:mysql://Plone/test", "sa", "");
        directory.add(SqlService.class, sqlService);
        return sqlService.getDb();
    }

    protected void checkDb(Key key, Field field, Object value, SqlConnection sqlConnection) {
        Assert.assertEquals(value, getNextValue(key, sqlConnection, field));
    }

    public Object getNextValue(Key key, SqlConnection sqlConnection, Field field) {
        GlobType globType = key.getGlobType();
        Constraint constraint = null;

        for (Field keyField : globType.getKeyFields()) {
            constraint = Constraints.and(constraint, Constraints.equalsObject(keyField, key.getValue(keyField)));
        }
        SelectBuilder queryBuilder = sqlConnection.getQueryBuilder(key.getGlobType(), constraint);
        Accessor accessor = queryBuilder.retrieveUnTyped(field);
        GlobStream globStream = queryBuilder.getQuery().execute();
        Assert.assertTrue(globStream.next());
        return accessor.getObjectValue();
    }

    protected void populate(SqlConnection connection, GlobStream stream) {
        Set<GlobType> types = new HashSet<GlobType>();
        for (Field field : stream.getFields()) {
            types.add(field.getGlobType());
        }
        CreateBuilder createBuilder = new MultiCreateBuilder(connection, types);
        Collection<Field> fields = stream.getFields();
        for (Field field : fields) {
            createBuilder.setObject(field, stream.getAccessor(field));
        }
        SqlRequest request = createBuilder.getRequest();
        while (stream.next()) {
            request.run();
        }
        connection.commit();
    }

}
