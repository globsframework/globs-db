package org.globsframework.sqlstreams.drivers.jdbc.request;

import org.globsframework.metamodel.GlobModel;
import org.globsframework.model.Glob;
import org.globsframework.model.Key;
import org.globsframework.model.KeyBuilder;
import org.globsframework.sqlstreams.model.DummyObject;
import org.globsframework.sqlstreams.SqlRequest;
import org.globsframework.sqlstreams.UpdateBuilder;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.globsframework.sqlstreams.constraints.impl.KeyConstraint;
import org.globsframework.sqlstreams.drivers.jdbc.DbServicesTestCase;
import org.globsframework.sqlstreams.model.DummyWithDateTime;
import org.globsframework.streams.DbStream;
import org.globsframework.streams.accessors.utils.ValueDoubleAccessor;
import org.globsframework.streams.accessors.utils.ValueIntegerAccessor;
import org.globsframework.xml.XmlGlobStreamReader;
import org.junit.Assert;
import org.junit.Test;

import java.time.*;
import java.time.temporal.ChronoField;
import java.util.Optional;

public class SqlUpdateBuilderTest extends DbServicesTestCase {

    @Test
    public void testUpdate() throws Exception {
        DbStream streamToWrite =
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true' password='zerzer'/>" +
                                "<dummyObject id='2' name='hello' value='0.0' present='true' password='gzsefd'/>", directory.get(GlobModel.class));
        populate(sqlConnection, streamToWrite);

        ValueIntegerAccessor keyValue = new ValueIntegerAccessor();
        UpdateBuilder updateBuilder = sqlConnection.getUpdateBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, keyValue));

        int nbDays = (int) LocalDate.parse("2017-12-03").getLong(ChronoField.EPOCH_DAY);
        updateBuilder.update(DummyObject.DATE, nbDays);

        updateBuilder.update(DummyObject.CREATED_AT, 123321L);

        ValueDoubleAccessor valueAccessor = new ValueDoubleAccessor(2.2);
        updateBuilder.updateUntyped(DummyObject.VALUE, valueAccessor);
        updateBuilder.updateUntyped(DummyObject.PASSWORD, "some blog".getBytes());

        SqlRequest updateRequest = updateBuilder.getRequest();
        Key key1 = KeyBuilder.newKey(DummyObject.TYPE, 1);

        keyValue.setValue(1);
        updateRequest.run();
        checkDb(key1, DummyObject.DATE, nbDays, sqlConnection);
        checkDb(key1, DummyObject.CREATED_AT, 123321L, sqlConnection);
        checkDb(key1, DummyObject.VALUE, 2.2, sqlConnection);
        Assert.assertEquals(new String((byte[]) getNextValue(key1, sqlConnection, DummyObject.PASSWORD)), "some blog");

        valueAccessor.setValue(3.3);
        updateRequest.run();
        checkDb(key1, DummyObject.VALUE, 3.3, sqlConnection);

        keyValue.setValue(2);
        Key key2 = KeyBuilder.newKey(DummyObject.TYPE, 2);
        updateRequest.run();
        checkDb(key2, DummyObject.VALUE, 3.3, sqlConnection);
    }


    @Test
    public void updateDateAndTime() {
        try {
            sqlConnection.emptyTable(DummyWithDateTime.TYPE);
        } catch(Exception e){
            //Assert.assertTrue(e.getMessage(), true);
        } finally {
            sqlConnection.commit();
        }

        sqlConnection.createTable(DummyWithDateTime.TYPE);
        sqlConnection.getCreateBuilder(DummyWithDateTime.TYPE)
                .set(DummyWithDateTime.uuid, "AAAAA")
                .set(DummyWithDateTime.date, LocalDate.of(2022, 10, 3))
                .set(DummyWithDateTime.created, ZonedDateTime.of(LocalDate.of(2022, 10, 3),
                        LocalTime.of(12, 0, 0), ZoneId.systemDefault()))
                .getRequest()
                .run();
        sqlConnection.commit();

        final LocalDate laure = LocalDate.of(1973, 10, 3);
        sqlConnection.getUpdateBuilder(DummyWithDateTime.TYPE, Constraints.equal(DummyWithDateTime.uuid, "AAAAA"))
                .update(DummyWithDateTime.date, laure)
                .update(DummyWithDateTime.created, ZonedDateTime.of(LocalDate.of(1973, 10, 3),
                        LocalTime.of(8, 0, 0), ZoneId.systemDefault()))
                .getRequest()
                .run();
        sqlConnection.commit();

        final Optional<Glob> aaaaa = sqlConnection.getQueryBuilder(DummyWithDateTime.TYPE, Constraints.equal(DummyWithDateTime.uuid, "AAAAA"))
                .selectAll()
                .getQuery()
                .executeOne();
        sqlConnection.commit();

        Assert.assertTrue(aaaaa.isPresent());
        String offsetId = ZoneId.systemDefault().getRules().getStandardOffset(Instant.now()).getId();

        Assert.assertEquals("{ \"_kind\":\"dummyWithDateTime\",\"uuid\":\"AAAAA\", \"created\":\"1973-10-03T08:00" +
                        offsetId +"[" + ZoneId.systemDefault() + "]\", \"date\":\"1973-10-03\"}",
                aaaaa.get().toString());
    }

    @Test
    public void testUpdateWithKey() throws Exception {
        DbStream streamToWrite =
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true'/>" +
                                "<dummyObject id='2' name='hello' value='0.0' present='true'/>", directory.get(GlobModel.class));
        populate(sqlConnection, streamToWrite);

        Key key1 = KeyBuilder.init(DummyObject.ID, 2).get();
        KeyConstraint keyAccessor = new KeyConstraint(DummyObject.TYPE);
        keyAccessor.setValue(key1);
        sqlConnection.getUpdateBuilder(DummyObject.TYPE, Constraints.keyEquals(keyAccessor))
                .update(DummyObject.NAME, "world")
                .getRequest()
                .run();
        checkDb(key1, DummyObject.NAME, "world", sqlConnection);
    }
}
