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
import org.globsframework.sql.utils.MultiCreateBuilder;
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
        sqlConnection.addColumn(DummyObject.VALUE);
        sqlConnection.createTable(DummyObject2.TYPE);
        emptyTable();
    }

    @After
    public void tearDown() throws Exception {
        try {
            sqlConnection.commitAndClose();
        } catch (Exception e) {
        }
        sqlConnection = sqlService.getDb();
        emptyTable();
        sqlConnection.commitAndClose();
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
//        sqlService = new JdbcSqlService("jdbc:postgresql://127.0.0.1:5432/glindaBackend", "glindaBackend", "glinda");
//    sqlService = new JdbcDriverBasedSqlService("jdbc:mysql://Plone/test", "sa", "");
// MB localtest HSQLDB
// sqlService = new JdbcSqlService("jdbc:hsqldb:http://localhost/", "SA", "");
// MB localtest postgree

        /*
            podman run --name test_glob \
    -v ~/tmp/.testGlob/postgres:/var/lib/postgresql/data \
    -e POSTGRES_USER=admin \
    -e POSTGRES_PASSWORD=DevTeam! \
    -p 5432:5432 \
    postgres:16

-e POSTGRES_HOST_AUTH_METHOD=trust \

         */
// sqlService = new JdbcSqlService("jdbc:postgresql://127.0.0.1:5432/postgres", "admin", "DevTeam!");
// MB localtest mysql
// sqlService = new JdbcSqlService("jdbc:mysql://127.0.0.1:3306/sys", "root", "DevTeam!");

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
