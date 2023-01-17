package org.globsframework.sqlstreams.drivers.jdbc.request;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.LongField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.metamodel.type.DataType;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.CreateBuilder;
import org.globsframework.sqlstreams.model.DummyObject;
import org.globsframework.model.KeyBuilder;
import org.globsframework.sqlstreams.drivers.jdbc.DbServicesTestCase;
import org.globsframework.sqlstreams.model.DummyObjectWithGlob;
import org.globsframework.streams.accessors.LongAccessor;
import org.globsframework.streams.accessors.utils.*;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

public class SqlCreateBuilderTest extends DbServicesTestCase {

    @Test
    public void testSimpleCreate() throws Exception {
        sqlConnection.createTable(DummyObject.TYPE);
        GlobType globType = sqlConnection.extractType(sqlService.getTableName(DummyObject.TYPE))
                .forceType(sqlService.getColumnName(DummyObject.CREATED_AT), DataType.Long).extract();

        Field timestampField = globType.getField(sqlService.getColumnName(DummyObject.CREATED_AT));

        Assert.assertTrue(timestampField.getDataType().name(), timestampField instanceof LongField);

        ZonedDateTime dateTime = ZonedDateTime.ofInstant(LocalDateTime.of(2019, 9, 15, 5, 10),
                ZoneOffset.UTC, ZoneId.systemDefault());

        sqlConnection.getCreateBuilder(DummyObject.TYPE)
                .set(DummyObject.ID, new ValueIntegerAccessor(1))
                .set(DummyObject.NAME, new ValueStringAccessor("hello"))
                .set(DummyObject.ALIAS, new ValueStringArrayAccessor(new String[]{"hello", "world"}))
                .set(DummyObject.PASSWORD, new ValueBlobAccessor("world".getBytes()))
                .set(DummyObject.CREATED_AT, 4324L)
                .set(DummyObject.REAL_DATE_TIME, dateTime)
                .getRequest()
                .run();
        checkDb(KeyBuilder.newKey(DummyObject.TYPE, 1), DummyObject.NAME, "hello", sqlConnection);
        checkDb(KeyBuilder.newKey(DummyObject.TYPE, 1), DummyObject.CREATED_AT, 4324L, sqlConnection);
        Assert.assertEquals("world",
                new String((byte[]) getNextValue(KeyBuilder.newKey(DummyObject.TYPE, 1), sqlConnection, DummyObject.PASSWORD)));

        Glob glob = sqlConnection.getQueryBuilder(DummyObject.TYPE)
                .selectAll()
                .getQuery().executeUnique();
        Assert.assertEquals(dateTime, glob.get(DummyObject.REAL_DATE_TIME));
        Assert.assertArrayEquals(new String[]{"hello", "world"}, glob.get(DummyObject.ALIAS));
    }

    @Ignore
    @Test
    public void checkAutoIncrementIsRetrieved() {
        sqlConnection.createTable(DummyObject.TYPE);
        CreateBuilder createBuilder = sqlConnection.getCreateBuilder(DummyObject.TYPE);
        LongAccessor keyGeneratedAccessor = createBuilder.getKeyGeneratedAccessor();
        createBuilder
                .set(DummyObject.NAME, "val1")
                .getRequest()
                .run();

        Long valId1 = keyGeneratedAccessor.getLong();
        Assert.assertNotNull(valId1);
    }

    public void setUp() throws Exception {
        super.setUp();
        try {
            sqlConnection.emptyTable(DummyObjectWithGlob.TYPE);
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage(), true);
        } finally {
            sqlConnection.commit();
        }
    }

    public void tearDown() throws Exception {
        super.tearDown();
        if(sqlConnection != null) {
            try {
                sqlConnection.emptyTable(DummyObjectWithGlob.TYPE);
            } catch (Exception e) {
                Assert.assertTrue(e.getMessage(), true);
            }
        }
    }

    @Test
    public void createGlobInField() {
        sqlConnection.createTable(DummyObjectWithGlob.TYPE);
        GlobType globType = sqlConnection.extractType(sqlService.getTableName(DummyObjectWithGlob.TYPE))
                .forceType(sqlService.getColumnName(DummyObjectWithGlob.arrayField), DataType.String)
                .forceType(sqlService.getColumnName(DummyObjectWithGlob.simple), DataType.String)
                .extract();

        Field arrayField = globType.getField(sqlService.getColumnName(DummyObjectWithGlob.arrayField));

        Assert.assertTrue(arrayField.getDataType().name(), arrayField instanceof StringField);

        sqlConnection.getCreateBuilder(DummyObjectWithGlob.TYPE)
                .set(DummyObjectWithGlob.ID, new ValueIntegerAccessor(1))
                .set(DummyObjectWithGlob.simple, new ValueGlobAccessor(DummyObjectWithGlob.TYPE.instantiate()
                        .set(DummyObjectWithGlob.ID, 2)
                ))
                .set(DummyObjectWithGlob.arrayField, new ValueGlobsAccessor(new Glob[]{DummyObjectWithGlob.TYPE.instantiate()
                        .set(DummyObjectWithGlob.ID, 3), DummyObjectWithGlob.TYPE.instantiate()
                        .set(DummyObjectWithGlob.ID, 4)}))
                .getRequest()
                .run();

        Glob glob = sqlConnection.getQueryBuilder(DummyObjectWithGlob.TYPE)
                .selectAll().getQuery().executeUnique();
        Assert.assertNotNull(glob.get(DummyObjectWithGlob.simple) );
        Assert.assertNotNull(glob.get(DummyObjectWithGlob.arrayField) );
        Assert.assertEquals(2, glob.get(DummyObjectWithGlob.simple).get(DummyObjectWithGlob.ID).intValue() );
        Assert.assertEquals(3, glob.get(DummyObjectWithGlob.arrayField)[0].get(DummyObjectWithGlob.ID).intValue() );
        Assert.assertEquals(4, glob.get(DummyObjectWithGlob.arrayField)[1].get(DummyObjectWithGlob.ID).intValue() );
    }
}
