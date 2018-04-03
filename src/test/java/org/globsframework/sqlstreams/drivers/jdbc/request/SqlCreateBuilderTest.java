package org.globsframework.sqlstreams.drivers.jdbc.request;

import org.globsframework.metamodel.DummyObject;
import org.globsframework.model.KeyBuilder;
import org.globsframework.sqlstreams.drivers.jdbc.DbServicesTestCase;
import org.globsframework.streams.accessors.utils.ValueBlobAccessor;
import org.globsframework.streams.accessors.utils.ValueIntegerAccessor;
import org.globsframework.streams.accessors.utils.ValueStringAccessor;
import org.junit.Assert;
import org.junit.Test;

public class SqlCreateBuilderTest extends DbServicesTestCase {

    @Test
    public void testSimpleCreate() throws Exception {
        sqlConnection.createTable(DummyObject.TYPE);
        sqlConnection.getCreateBuilder(DummyObject.TYPE)
                .set(DummyObject.ID, new ValueIntegerAccessor(1))
                .set(DummyObject.NAME, new ValueStringAccessor("hello"))
                .set(DummyObject.PASSWORD, new ValueBlobAccessor("world".getBytes()))
                .getRequest()
                .run();
        checkDb(KeyBuilder.newKey(DummyObject.TYPE, 1), DummyObject.NAME, "hello", sqlConnection);
        Assert.assertEquals("world",
                new String((byte[]) getNextValue(KeyBuilder.newKey(DummyObject.TYPE, 1), sqlConnection, DummyObject.PASSWORD)));
    }
}
