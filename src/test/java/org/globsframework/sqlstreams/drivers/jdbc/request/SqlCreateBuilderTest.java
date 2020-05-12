package org.globsframework.sqlstreams.drivers.jdbc.request;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.LongField;
import org.globsframework.metamodel.type.DataType;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.model.DummyObject;
import org.globsframework.model.KeyBuilder;
import org.globsframework.sqlstreams.drivers.jdbc.DbServicesTestCase;
import org.globsframework.streams.accessors.utils.ValueBlobAccessor;
import org.globsframework.streams.accessors.utils.ValueIntegerAccessor;
import org.globsframework.streams.accessors.utils.ValueStringAccessor;
import org.globsframework.streams.accessors.utils.ValueStringArrayAccessor;
import org.junit.Assert;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Arrays;

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
}
