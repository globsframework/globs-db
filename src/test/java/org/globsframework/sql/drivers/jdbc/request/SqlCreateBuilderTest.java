package org.globsframework.sql.drivers.jdbc.request;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.metamodel.fields.LongField;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.metamodel.type.DataType;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.KeyBuilder;
import org.globsframework.core.streams.accessors.IntegerAccessor;
import org.globsframework.core.streams.accessors.utils.*;
import org.globsframework.json.GSonUtils;
import org.globsframework.sql.BatchSqlRequest;
import org.globsframework.sql.CreateBuilder;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.drivers.jdbc.DbServicesTestCase;
import org.globsframework.sql.model.DummyObject;
import org.globsframework.sql.model.DummyObjectWithGlob;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

public class SqlCreateBuilderTest extends DbServicesTestCase {

    @Test
    public void testSimpleCreate() throws Exception {
        sqlConnection.createTable(DummyObject.TYPE);
        GlobType globType = sqlConnection.extractType(sqlService.getTableName(DummyObject.TYPE, false))
                .forceType(sqlService.getColumnName(DummyObject.CREATED_AT, false), DataType.Long).extract();

        Field timestampField = globType.getField(sqlService.getColumnName(DummyObject.CREATED_AT, false));

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
                .apply();
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
        IntegerAccessor keyGeneratedAccessor = createBuilder.getKeyGeneratedAccessor(DummyObject.ID);
        createBuilder
                .set(DummyObject.NAME, "val1")
                .getRequest()
                .apply();

        Integer valId1 = keyGeneratedAccessor.getInteger();
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
        if (sqlConnection != null) {
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
        GlobType globType = sqlConnection.extractType(sqlService.getTableName(DummyObjectWithGlob.TYPE, false))
                .forceType(sqlService.getColumnName(DummyObjectWithGlob.arrayField, false), DataType.String)
                .forceType(sqlService.getColumnName(DummyObjectWithGlob.simple, false), DataType.String)
                .extract();

        Field arrayField = globType.getField(sqlService.getColumnName(DummyObjectWithGlob.arrayField, false));

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
                .apply();

        Glob glob = sqlConnection.getQueryBuilder(DummyObjectWithGlob.TYPE)
                .selectAll().getQuery().executeUnique();
        Assert.assertNotNull(glob.get(DummyObjectWithGlob.simple));
        Assert.assertNotNull(glob.get(DummyObjectWithGlob.arrayField));
        Assert.assertEquals(2, glob.get(DummyObjectWithGlob.simple).get(DummyObjectWithGlob.ID).intValue());
        Assert.assertEquals(3, glob.get(DummyObjectWithGlob.arrayField)[0].get(DummyObjectWithGlob.ID).intValue());
        Assert.assertEquals(4, glob.get(DummyObjectWithGlob.arrayField)[1].get(DummyObjectWithGlob.ID).intValue());
    }

    @Test
    public void testBulkInsertAndUpdate() {
        sqlConnection.createTable(DummyObject.TYPE);
        final ValueIntegerAccessor accessor = new ValueIntegerAccessor(1);
        final ValueStringAccessor hello = new ValueStringAccessor("hello");
        final BatchSqlRequest bulkRequest = sqlConnection.getCreateBuilder(DummyObject.TYPE)
                .set(DummyObject.ID, accessor)
                .set(DummyObject.NAME, hello)
                .set(DummyObject.CREATED_AT, 4324L)
                .getBulkRequest();
        for (int i = 0; i < 100; i++){
            accessor.setValue(i);
            bulkRequest.addBatch();
            if (i % 10 == 0) {
                bulkRequest.applyBatch();
            }
        }
        bulkRequest.applyBatch();
        sqlConnection.commit();

        {
            final List<Glob> globs = sqlConnection.getQueryBuilder(DummyObject.TYPE)
                    .selectAll().getQuery().executeAsGlobs();
            Assert.assertEquals(100, globs.size());
        }

        final ValueIntegerAccessor idAccessor = new ValueIntegerAccessor();
        final ValueStringAccessor nameAccessor = new ValueStringAccessor();
        final BatchSqlRequest bulkUpdate = sqlConnection.getUpdateBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, idAccessor))
                .update(DummyObject.NAME, nameAccessor)
                .getBulkRequest();

        for (int i = 0; i <100; i++) {
            idAccessor.setValue(i);
            nameAccessor.setValue("Hello " + i);
            bulkUpdate.addBatch();
        }
        bulkUpdate.applyBatch();
        {
            final List<Glob> globs = sqlConnection.getQueryBuilder(DummyObject.TYPE)
                    .selectAll()
                    .orderAsc(DummyObject.ID).getQuery().executeAsGlobs();
            Assert.assertEquals(100, globs.size());
            Assert.assertEquals("Hello 10", globs.get(10).get(DummyObject.NAME));
        }

    }
}
