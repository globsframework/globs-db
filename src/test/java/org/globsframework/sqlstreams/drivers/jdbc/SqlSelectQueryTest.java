package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.json.GSonUtils;
import org.globsframework.metamodel.GlobModel;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.KeyField;
import org.globsframework.metamodel.annotations.Target;
import org.globsframework.metamodel.annotations.Targets;
import org.globsframework.metamodel.fields.*;
import org.globsframework.sqlstreams.model.DummyObject;
import org.globsframework.sqlstreams.model.DummyObject2;
import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.SelectBuilder;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.globsframework.sqlstreams.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sqlstreams.exceptions.SqlException;
import org.globsframework.streams.DbStream;
import org.globsframework.streams.accessors.GlobAccessor;
import org.globsframework.streams.accessors.GlobsAccessor;
import org.globsframework.streams.accessors.IntegerAccessor;
import org.globsframework.streams.accessors.StringAccessor;
import org.globsframework.streams.accessors.utils.ValueIntegerAccessor;
import org.globsframework.utils.Ref;
import org.globsframework.utils.Utils;
import org.globsframework.xml.XmlGlobStreamReader;
import org.junit.Assert;
import org.junit.Test;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Collections;
import java.util.stream.Stream;

import static org.globsframework.sqlstreams.constraints.Constraints.and;
import static org.junit.Assert.*;

public class SqlSelectQueryTest extends DbServicesTestCase {

    @Test
    public void testSimpleRequest() throws Exception {
        DbStream streamToWrite =
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true'/>", directory.get(GlobModel.class));
        populate(sqlConnection, streamToWrite);

        Ref<IntegerAccessor> idAccessor = new Ref<IntegerAccessor>();
        Ref<StringAccessor> nameAccessor = new Ref<StringAccessor>();
        SelectQuery query =
                sqlConnection.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 1))
                        .select(DummyObject.ID, idAccessor)
                        .select(DummyObject.NAME, nameAccessor)
                        .select(DummyObject.PRESENT)
                        .select(DummyObject.COUNT)
                        .select(DummyObject.VALUE).getQuery();

        DbStream requestStream = query.execute();
        assertTrue(requestStream.next());
        assertEquals(1, idAccessor.get().getValue(0));
        assertEquals("hello", nameAccessor.get().getString());
        assertNull(requestStream.getAccessor(DummyObject.COUNT).getObjectValue());
        assertEquals(1.1, requestStream.getAccessor(DummyObject.VALUE).getObjectValue());
        assertEquals(true, requestStream.getAccessor(DummyObject.PRESENT).getObjectValue());
        assertFalse(requestStream.next());
    }

    @Test
    public void testMultipleExecute() throws Exception {
        SqlConnection sqlConnection = init();
        ValueIntegerAccessor value = new ValueIntegerAccessor(1);
        SelectQuery query =
                sqlConnection.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, value))
                        .select(DummyObject.NAME)
                        .getQuery();
        DbStream hellotream = query.execute();
        assertTrue(hellotream.next());
        assertEquals("hello", hellotream.getAccessor(DummyObject.NAME).getObjectValue());

        value.setValue(2);
        DbStream worldStream = query.execute();
        assertTrue(worldStream.next());
        assertEquals("world", worldStream.getAccessor(DummyObject.NAME).getObjectValue());

    }

    @Test
    public void testAnd() throws Exception {
        SqlConnection sqlConnection = init();

        GlobList list =
                sqlConnection.getQueryBuilder(DummyObject.TYPE,
                        and(Constraints.equal(DummyObject.NAME, "hello"),
                                Constraints.equal(DummyObject.ID, 1)))
                        .selectAll()
                        .getQuery()
                        .executeAsGlobs();
        assertEquals(1, list.size());
        assertEquals(1, list.get(0).get(DummyObject.ID).intValue());
    }

    @Test
    public void testNullAnd() throws Exception {
        SqlConnection sqlConnection = init();
        sqlConnection.getQueryBuilder(DummyObject.TYPE, and(null,
                Constraints.equal(DummyObject.ID, 1)))
                .withKeys()
                .getQuery().executeUnique();
    }

    @Test
    public void testNullOr() throws Exception {
        SqlConnection sqlConnection = init();
        sqlConnection.getQueryBuilder(DummyObject.TYPE, Constraints.or(null,
                Constraints.equal(DummyObject.ID, 1))).withKeys()
                .getQuery().executeUnique();
    }

    @Test
    public void testJointure() throws Exception {
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true'/>" +
                                "<dummyObject id='3' name='world' value='2.2' present='false'/>", directory.get(GlobModel.class)));
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject2 id='2' label='world'/>", directory.get(GlobModel.class)));

        Glob glob = execute(Constraints.fieldEqual(DummyObject.NAME, DummyObject2.LABEL));
        assertEquals(glob.get(DummyObject.ID).intValue(), 3);
    }

    @Test
    public void testLessBigger() throws Exception {
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true' />" +
                                "<dummyObject id='2' name='world' value='2.2' present='false' />", directory.get(GlobModel.class)));

        assertEquals(1, execute(Constraints.lessUncheck(DummyObject.VALUE, 1.2)).get(DummyObject.ID).intValue());
        assertEquals(1, execute(Constraints.lessUncheck(DummyObject.VALUE, 1.1)).get(DummyObject.ID).intValue());
        assertEquals(1, execute(Constraints.LesserUnchecked(DummyObject.VALUE, 1.2)).get(DummyObject.ID).intValue());
        assertEquals(2, execute(Constraints.greaterUnchecked(DummyObject.VALUE, 1.2)).get(DummyObject.ID).intValue());
        assertEquals(2, execute(Constraints.greaterUnchecked(DummyObject.VALUE, 2.2)).get(DummyObject.ID).intValue());
        assertEquals(2, execute(Constraints.strictlyGreater(DummyObject.VALUE, 1.2)).get(DummyObject.ID).intValue());
        checkEmpty(Constraints.strictlyGreater(DummyObject.VALUE, 2.2));
        checkEmpty(Constraints.LesserUnchecked(DummyObject.VALUE, 1.1));
        checkEmpty(Constraints.strictlyGreater(DummyObject.VALUE, 3.2));
        checkEmpty(Constraints.LesserUnchecked(DummyObject.VALUE, 0.1));
        checkEmpty(Constraints.greaterUnchecked(DummyObject.VALUE, 3.2));
        checkEmpty(Constraints.lessUncheck(DummyObject.VALUE, 0.1));
    }

    @Test
    public void testMixedExcecuteOnSameQueryIsNotsuported() throws Exception {

        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true'/>" +
                                "<dummyObject id='3' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='4' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='5' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='6' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='7' name='world' value='2.2' present='false'/>", directory.get(GlobModel.class)));
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject2 id='2' label='world'/>", directory.get(GlobModel.class)));

        Ref<IntegerAccessor> ref = new Ref<IntegerAccessor>();
        SelectQuery query = sqlConnection.getQueryBuilder(DummyObject.TYPE,
                Constraints.fieldEqual(DummyObject.NAME, DummyObject2.LABEL))
                .select(DummyObject.ID, ref).getQuery();
        final DbStream firstDbStream = query.execute();
        final IntegerAccessor firstAccessor = ref.get();
        DbStream secondDbStream = query.execute();
        IntegerAccessor secondAccessor = ref.get();
        assertFails(() -> {
            firstDbStream.next();
            firstAccessor.getValue(0);
        }, SqlException.class);
    }

    public static void assertFails(Runnable functor, Class<? extends Exception> expectedException) {
        try {
            functor.run();
        } catch (Exception e) {
            if (!e.getClass().isAssignableFrom(expectedException)) {
                StringWriter writer = new StringWriter();
                e.printStackTrace(new PrintWriter(writer));
                Assert.fail(expectedException.getName() + " expected but was " + e.getClass().getName() + "\n" +
                        writer.toString());
            }
        }
    }

    @Test
    public void testInConstraint() throws Exception {
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true'/>" +
                                "<dummyObject id='3' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='4' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='5' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='6' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='7' name='world' value='2.2' present='false'/>", directory.get(GlobModel.class)));
        Integer[] values = {1, 2, 3, 4, 5};
        GlobList list = sqlConnection.getQueryBuilder(DummyObject.TYPE,
                Constraints.in(DummyObject.ID, Utils.set(values))).withKeys().getQuery().executeAsGlobs();
        assertEquals(4, list.size());
    }

    @Test
    public void OrderAndLimit() {
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true'/>" +
                                "<dummyObject id='3' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='4' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='5' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='6' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='7' name='world' value='2.2' present='false'/>", directory.get(GlobModel.class)));
        Integer[] values = {1, 2, 3, 4, 5};
        GlobList list = sqlConnection.getQueryBuilder(DummyObject.TYPE,
                Constraints.in(DummyObject.ID, Utils.set(values)))
                .withKeys()
                .orderDesc(DummyObject.ID).orderAsc(DummyObject.VALUE)
                .top(1)
                .getQuery().executeAsGlobs();
        assertEquals(1, list.size());
        assertEquals(5, list.get(0).get(DummyObject.ID).intValue());
    }

    @Test
    public void testNotEqual() throws Exception {
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true'/>" +
                                "<dummyObject id='3' name='world' value='2.2' present='false'/>", directory.get(GlobModel.class)));

        Glob glob = execute(Constraints.notEqual(DummyObject.NAME, "hello"));
        assertEquals(glob.get(DummyObject.ID).intValue(), 3);
    }

    @Test
    public void testContainsOrNot() throws Exception {
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true'/>" +
                                "<dummyObject id='3' name='world' value='2.2' present='false'/>", directory.get(GlobModel.class)));

        Glob glob = execute(Constraints.contains(DummyObject.NAME, "hello"));
        assertEquals(glob.get(DummyObject.ID).intValue(), 1);

        glob = execute(Constraints.notContains(DummyObject.NAME, "hello"));
        assertEquals(glob.get(DummyObject.ID).intValue(), 3);
    }


    private SqlConnection init() {
        DbStream streamToWrite =
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1'  name='hello' value='1.1' present='true'/>" +
                                "<dummyObject id='2'  name='world' value='2.2' present='false'/>", directory.get(GlobModel.class));
        populate(sqlConnection, streamToWrite);
        return sqlConnection;
    }

    @Test
    public void distinct() {
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true'/>" +
                                "<dummyObject id='3' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='4' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='5' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='6' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='7' name='world' value='2.2' present='false'/>", directory.get(GlobModel.class)));
        GlobList list = ((SqlQueryBuilder) sqlConnection.getQueryBuilder(DummyObject.TYPE)
                .select(DummyObject.NAME))
                .distinct(Collections.singletonList(DummyObject.NAME))
                .getQuery().executeAsGlobs();
        assertEquals(2, list.size());
    }

    @Test
    public void closeIsCallOnStream() {
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true'/>" +
                                "<dummyObject id='2' name='hello' value='1.2' present='true'/>", directory.get(GlobModel.class)));
        try (Stream<Glob> globStream = sqlConnection.getQueryBuilder(DummyObject.TYPE)
                .select(DummyObject.NAME)
                .selectAll()
                .getQuery()
                .executeAsGlobStream()) {
            assertTrue(globStream.findFirst().isPresent());
        }
// check done with debugger : close is called thinks to the try block
    }

    private void checkEmpty(Constraint constraint) {
        assertTrue(sqlConnection.getQueryBuilder(DummyObject.TYPE, constraint).withKeys().getQuery().executeAsGlobs().isEmpty());
    }

    private Glob execute(Constraint constraint) {
        return sqlConnection.getQueryBuilder(DummyObject.TYPE, constraint)
                .withKeys()
                .getQuery().executeUnique();
    }

    public static class ValueType {
        public static GlobType TYPE;

        public static IntegerField DATE;

        public static DoubleField VALUE;

        static {
            GlobTypeLoaderFactory.create(ValueType.class, true).load();
        }
    }

    public static class ValueType2 {
        public static GlobType TYPE;

        public static IntegerField DATE;

        public static IntegerField VALUE;

        static {
            GlobTypeLoaderFactory.create(ValueType2.class, true).load();
        }
    }

    public static class GlobWithGlobType {
        public static GlobType TYPE;

        @KeyField
        public static IntegerField ID;

        @Target(ValueType.class)
        public static GlobField VALUE;

        @Target(ValueType.class)
        public static GlobArrayField VALUES__2;

        @Targets({ValueType.class, ValueType2.class})
        public static GlobUnionField ANY_TYPE;

        @Targets({ValueType.class, ValueType2.class})
        public static GlobArrayUnionField ANY_TYPES;

        static {
            GlobTypeLoaderFactory.create(GlobWithGlobType.class, true).load();
        }
    }

    @Test
    public void checkGlobAreRetrieved() {
        SqlConnection db = sqlService.getDb();
        db.createTable(GlobWithGlobType.TYPE);
        db.getCreateBuilder(GlobWithGlobType.TYPE)
                .set(GlobWithGlobType.ID, 1)
                .set(GlobWithGlobType.VALUE, ValueType.TYPE.instantiate().set(ValueType.DATE, 1).set(ValueType.VALUE, 3.14))
                .set(GlobWithGlobType.VALUES__2, new Glob[]{ValueType.TYPE.instantiate().set(ValueType.DATE, 1).set(ValueType.VALUE, 3.14), ValueType.TYPE.instantiate().set(ValueType.DATE, 2).set(ValueType.VALUE, 6.28)})
                .set(GlobWithGlobType.ANY_TYPE, ValueType.TYPE.instantiate().set(ValueType.DATE, 1).set(ValueType.VALUE, 3.14))
                .set(GlobWithGlobType.ANY_TYPES, new Glob[]{ValueType.TYPE.instantiate().set(ValueType.DATE, 1).set(ValueType.VALUE, 3.14), ValueType.TYPE.instantiate().set(ValueType.DATE, 2).set(ValueType.VALUE, 6.28)})
                .getRequest().run();

        SelectBuilder queryBuilder = db.getQueryBuilder(GlobWithGlobType.TYPE);
        GlobAccessor globAccessor = queryBuilder.retrieve(GlobWithGlobType.VALUE);
        GlobsAccessor globsAccessor = queryBuilder.retrieve(GlobWithGlobType.VALUES__2);

        Glob glob = queryBuilder.getQuery().executeUnique();

        Assert.assertEquals("{\"date\":1,\"value\":3.14}", GSonUtils.encode(glob.get(GlobWithGlobType.VALUE), false));
        Assert.assertEquals("{\"_kind\":\"valueType\",\"date\":1,\"value\":3.14}", GSonUtils.encode(glob.get(GlobWithGlobType.VALUE), true));
        Assert.assertEquals("[{\"date\":1,\"value\":3.14},{\"date\":2,\"value\":6.28}]", GSonUtils.encode(glob.get(GlobWithGlobType.VALUES__2), false));
        Assert.assertEquals("[{\"_kind\":\"valueType\",\"date\":1,\"value\":3.14},{\"_kind\":\"valueType\",\"date\":2,\"value\":6.28}]", GSonUtils.encode(glob.get(GlobWithGlobType.VALUES__2), true));

    }

    @Test
    public void checkMinMax(){
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' count='1' present='true'/>" +
                                "<dummyObject id='3' name='world' count='5' present='false'/>" +
                                "<dummyObject id='4' name='world' count='6' present='false'/>" +
                                "<dummyObject id='5' name='world' count='2' present='false'/>" +
                                "<dummyObject id='6' name='world' count='4' present='false'/>" +
                                "<dummyObject id='7' name='world' count='2' present='false'/>", directory.get(GlobModel.class)));
        SelectBuilder queryBuilder = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        IntegerAccessor max = queryBuilder.max(DummyObject.COUNT);
        IntegerAccessor min = queryBuilder.min(DummyObject.COUNT);
        Assert.assertTrue(queryBuilder.getQuery().execute().next());
        Assert.assertEquals(6, max.getInteger().intValue());
        Assert.assertEquals(1, min.getInteger().intValue());

    }
}
