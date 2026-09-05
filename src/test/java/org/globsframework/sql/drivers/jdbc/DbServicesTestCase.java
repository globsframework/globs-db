package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.directory.DefaultDirectory;
import org.globsframework.core.directory.Directory;
import org.globsframework.core.metamodel.GlobModel;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.metamodel.impl.DefaultGlobModel;
import org.globsframework.core.model.Key;
import org.globsframework.core.streams.GlobStream;
import org.globsframework.core.streams.accessors.Accessor;
import org.globsframework.sql.*;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.model.DummyObject;
import org.globsframework.sql.model.DummyObject2;
import org.globsframework.sql.testdb.TestDb;
import org.globsframework.sql.utils.MultiCreateBuilder;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public abstract class DbServicesTestCase {
    private static final Logger LOGGER = LoggerFactory.getLogger(DbServicesTestCase.class);

    protected JdbcSqlService sqlService;
    protected SqlConnection sqlConnection;
    protected DefaultGlobModel globModel;
    protected Directory directory = new DefaultDirectory();

    @Before
    public void setUp() throws Exception {
        globModel = new DefaultGlobModel(DummyObject.TYPE, DummyObject2.TYPE);
        sqlConnection = initDb();
        sqlConnection.createTable(DummyObject.TYPE);
        sqlConnection.addColumn(DummyObject.VALUE);
        sqlConnection.createTable(DummyObject2.TYPE);
        emptyTable();
    }

    @After
    public void tearDown() throws Exception {
        if (sqlService == null) {
            // setUp did not get as far as a service: skipped backend, or a failure worth surfacing
            // on its own rather than behind a NullPointerException from here
            return;
        }
        try {
            if (sqlConnection != null) {
                sqlConnection.commitAndClose();
            }
            sqlConnection = sqlService.getDb();
            emptyTable();
            sqlConnection.commitAndClose();
        } catch (Exception e) {
            LOGGER.warn("Cleanup failed", e);
        } finally {
            sqlConnection = null;
            sqlService.close();
            sqlService = null;
        }
    }

    private void emptyTable() {
        sqlConnection.emptyTable(DummyObject.TYPE);
        sqlConnection.emptyTable(DummyObject2.TYPE);
        sqlConnection.commit();
    }

    private SqlConnection initDb() {
        directory.add(GlobModel.class, globModel);
        // HSQLDB in memory by default; -Dglobs.test.db=postgresql runs the same suite against a
        // containerized PostgreSQL. See TestDb.
        sqlService = TestDb.createService();
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
            request.apply();
        }
        connection.commit();
    }

}
