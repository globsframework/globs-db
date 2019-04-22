package org.globsframework.sqlstreams.drivers.mongodb;

import com.github.fakemongo.junit.FongoAsyncRule;
import com.github.fakemongo.junit.FongoRule;
import com.mongodb.Block;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.bson.types.ObjectId;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoader;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.KeyField;
import org.globsframework.metamodel.annotations.Target;
import org.globsframework.metamodel.fields.*;
import org.globsframework.metamodel.index.MultiFieldUniqueIndex;
import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.model.KeyBuilder;
import org.globsframework.model.MutableGlob;
import org.globsframework.model.format.GlobPrinter;
import org.globsframework.model.repository.DefaultGlobRepository;
import org.globsframework.sqlstreams.SelectBuilder;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.annotations.typed.TypedDbRef;
import org.globsframework.sqlstreams.annotations.typed.TypedIsDbKey;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.globsframework.sqlstreams.drivers.jdbc.SqlSelectQueryTest;
import org.globsframework.streams.accessors.GlobAccessor;
import org.globsframework.streams.accessors.GlobsAccessor;
import org.globsframework.utils.Ref;
import org.globsframework.utils.Utils;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;

import static org.globsframework.sqlstreams.drivers.mongodb.MongoSelectTest.DummyObject.*;

public class MongoSelectTest {
    @Rule
    public FongoRule fongoRule = new FongoRule();
    @Rule
    public FongoAsyncRule fongoAsyncRule = new FongoAsyncRule();

    @Test
    public void Select() {
        InitDb initDb = new InitDb().invoke();
        MongoDatabase database = initDb.getDatabase();
        MongoDbService sqlService = initDb.getSqlService();

        SqlConnection mangoDbConnection = new MongoDbConnection(database, sqlService);
        GlobList globs = mangoDbConnection.getQueryBuilder(DummyObject.TYPE)
                .selectAll()
                .getQuery()
                .executeAsGlobs();
        DefaultGlobRepository globRepository = new DefaultGlobRepository();
        globRepository.add(globs);

        Assert.assertEquals(globRepository.get(KeyBuilder.newKey(DummyObject.TYPE, 1)).get(DummyObject.NAME), "name 1");
        Assert.assertEquals(globRepository.get(KeyBuilder.newKey(DummyObject.TYPE, 2)).get(DummyObject.NAME), "name 2");
        Assert.assertEquals(globRepository.get(KeyBuilder.newKey(DummyObject.TYPE, 3)).get(VALUE), 3.14 * 3, 0.01);
    }

    @Test
    public void IsNullIsExist() {
        InitDb initDb = new InitDb().invoke();
        MongoDatabase database = initDb.getDatabase();
        MongoDbService sqlService = initDb.getSqlService();

        SqlConnection mangoDbConnection = new MongoDbConnection(database, sqlService);
        GlobList globs = mangoDbConnection.getQueryBuilder(DummyObject.TYPE, Constraints.isNull(DummyObject.NAME_2))
                .selectAll()
                .getQuery()
                .executeAsGlobs();
        Assert.assertEquals(3, globs.size());

        globs = mangoDbConnection.getQueryBuilder(DummyObject.TYPE, Constraints.isNotNull(DummyObject.NAME_2))
                .selectAll()
                .getQuery()
                .executeAsGlobs();
        Assert.assertEquals(1, globs.size());
    }


    @Test
    public void contains() {
        InitDb initDb = new InitDb().invoke();
        MongoDatabase database = initDb.getDatabase();
        MongoDbService sqlService = initDb.getSqlService();

        SqlConnection mangoDbConnection = new MongoDbConnection(database, sqlService);
        GlobList globs = mangoDbConnection.getQueryBuilder(DummyObject.TYPE, Constraints.contains(DummyObject.NAME, "2"))
                .selectAll()
                .getQuery()
                .executeAsGlobs();
        Assert.assertEquals(1, globs.size());

        globs = mangoDbConnection.getQueryBuilder(DummyObject.TYPE, Constraints.notContains(DummyObject.NAME, "2"))
                .selectAll()
                .getQuery()
                .executeAsGlobs();
        Assert.assertEquals(3, globs.size());
    }

    @Test
    public void notIn() {
        InitDb initDb = new InitDb().invoke();
        MongoDatabase database = initDb.getDatabase();
        MongoDbService sqlService = initDb.getSqlService();

        SqlConnection mangoDbConnection = new MongoDbConnection(database, sqlService);
        GlobList globs = mangoDbConnection.getQueryBuilder(DummyObject.TYPE, Constraints.notIn(DummyObject.NAME, Utils.set("name 1", "name 2")))
                .selectAll()
                .getQuery()
                .executeAsGlobs();
        Assert.assertEquals(2, globs.size());
        Assert.assertTrue(globs.stream()
                .map(g -> g.get(DummyObject.NAME))
                .anyMatch(s -> s.equals("name 3")));
    }

    @Test
    public void orderAndLimit() {
        InitDb initDb = new InitDb().invoke();
        MongoDatabase database = initDb.getDatabase();
        MongoDbService sqlService = initDb.getSqlService();
        SqlConnection mangoDbConnection = new MongoDbConnection(database, sqlService);

        GlobList sortedFirstGlob = mangoDbConnection.getQueryBuilder(DummyObject.TYPE)
                .orderDesc(VALUE)
                .orderAsc(NAME)
                .top(1)
                .selectAll()
                .getQuery().executeAsGlobs();

        Assert.assertEquals(1, sortedFirstGlob.size());
        Assert.assertEquals(4, sortedFirstGlob.get(0).get(ID).intValue());
    }

    @Test
    public void testInOp() {
        InitDb initDb = new InitDb().invoke();
        MongoDatabase database = initDb.getDatabase();
        MongoDbService sqlService = initDb.getSqlService();
        SqlConnection mangoDbConnection = new MongoDbConnection(database, sqlService);

        GlobList sortedFirstGlob = mangoDbConnection.getQueryBuilder(DummyObject.TYPE, Constraints.in(DummyObject.NAME, Utils.set("name 1", "name 3")))
                .selectAll()
                .orderAsc(DummyObject.ID)
                .getQuery().executeAsGlobs();

        Assert.assertEquals(2, sortedFirstGlob.size());
        Assert.assertEquals(1, sortedFirstGlob.get(0).get(ID).intValue());
        Assert.assertEquals(3, sortedFirstGlob.get(1).get(ID).intValue());
    }

    @Test
    public void checkIndexCreation() {
        MongoDatabase database = fongoRule.getDatabase();
        com.mongodb.client.MongoCollection<Document> globMongoCollection = database.getCollection(DummyObject.TYPE.getName(), Document.class);
        MongoDbService sqlService = new MongoDbService(database);
        MongoUtils.createIndexIfNeeded(globMongoCollection, Collections.singleton(NAME_INDEX), sqlService);
        Ref<Boolean> future = new Ref<>();
        globMongoCollection.listIndexes().forEach((Block<? super Document>) document -> {
            if (MongoUtils.contain(NAME_INDEX, document, sqlService)) {
                future.set(Boolean.TRUE);
            }
        });
        Assert.assertTrue(future.get());
    }

    static public class InnerValue {
        public static GlobType TYPE;

        public static IntegerField DATE;

        public static DoubleField VALUE;

        static {
            GlobTypeLoaderFactory.create(InnerValue.class)
                    .load();
        }
    }

    static public class DummyObject {
        public static GlobType TYPE;

        @TypedIsDbKey
        public static StringField UUID;

        @KeyField
        public static IntegerField ID;

        public static DoubleField VALUE;

        public static StringField NAME;

        public static StringField NAME_2;

        @Target(InnerValue.class)
        public static GlobField LAST;

//        @Target(InnerValue.class)
//        public static GlobArrayField HISTO;

        public static MultiFieldUniqueIndex NAME_INDEX;

        static {
            GlobTypeLoader globTypeLoader = GlobTypeLoaderFactory.create(DummyObject.class);
            globTypeLoader.load();
            globTypeLoader.defineMultiFieldUniqueIndex(NAME_INDEX, NAME, NAME_2);
        }
    }

    static public class DummyObjectWithRef {
        public static GlobType TYPE;

        @TypedIsDbKey
        public static StringField UUID;

        @KeyField
        public static IntegerField ID;

        @TypedDbRef(to = "DummyObject")
        @KeyField
        public static StringField NAME;

        public static MultiFieldUniqueIndex NAME_INDEX;

        static {
            GlobTypeLoader globTypeLoader = GlobTypeLoaderFactory.create(DummyObjectWithRef.class);
            globTypeLoader.load();
        }
    }

    private class InitDb {
        private com.mongodb.client.MongoDatabase database;
        private MongoDbService sqlService;

        public com.mongodb.client.MongoDatabase getDatabase() {
            return database;
        }

        public MongoDbService getSqlService() {
            return sqlService;
        }

        public InitDb invoke() {
            return invoke(new MongoDbService.DefaultUpdateAdapterFactory());
        }

        public InitDb invoke(MongoDbService.UpdateAdapterFactory updateAdapterFactory) {
            database = fongoRule.getDatabase();
            sqlService = new MongoDbService(database, updateAdapterFactory);
            sqlService.getDb().populate(new GlobList(
                    DummyObject.TYPE.instantiate()
                            .set(DummyObject.ID, 1)
                            .set(DummyObject.NAME, "name 1")
                            .set(DummyObject.NAME_2, "second name")
                            .set(VALUE, 3.14),
                    DummyObject.TYPE.instantiate()
                            .set(DummyObject.ID, 2)
                            .set(DummyObject.NAME, "name 2")
                            .set(VALUE, 3.14 * 2.),
                    DummyObject.TYPE.instantiate()
                            .set(DummyObject.ID, 3)
                            .set(DummyObject.NAME, "name 3")
                            .set(VALUE, 3.14 * 3.),
                    DummyObject.TYPE.instantiate()
                            .set(DummyObject.ID, 4)
                            .set(DummyObject.NAME, "my name")
                            .set(VALUE, 3.14 * 3.),
                    DummyObjectWithRef.TYPE.instantiate()
                            .set(DummyObjectWithRef.ID, 1)
                            .set(DummyObjectWithRef.NAME, "5a9d63a875e8c98bb09ee1d3")));
            return this;
        }
    }

    public static class ValueType {
        public static GlobType TYPE;

        public static IntegerField DATE;

        public static DoubleField VALUE;

        static {
            GlobTypeLoaderFactory.create(ValueType.class).load();
        }
    }

    public static class GlobWithGlobType {
        public static GlobType TYPE;

        @KeyField
        public static StringField ID;

        @Target(ValueType.class)
        public static GlobField VALUE;

        @Target(ValueType.class)
        public static GlobArrayField VALUES;

        static {
            GlobTypeLoaderFactory.create(GlobWithGlobType.class).load();
        }
    }

    @Test
    public void checkGlobAreRetrieved() {
        MongoDatabase database = fongoRule.getDatabase();
        MongoDbService sqlService = new MongoDbService(database, new MongoDbService.DefaultUpdateAdapterFactory());
        SqlConnection db = sqlService.getDb();
        MutableGlob val1 = ValueType.TYPE.instantiate().set(ValueType.DATE, 1).set(ValueType.VALUE, 3.14);
        MutableGlob val2 = ValueType.TYPE.instantiate().set(ValueType.DATE, 2).set(ValueType.VALUE, 6.28);
        db.getCreateBuilder(GlobWithGlobType.TYPE)
                .set(GlobWithGlobType.ID, new ObjectId().toHexString())
                .set(GlobWithGlobType.VALUE, val1)
                .set(GlobWithGlobType.VALUES, new Glob[]{val1, val2})
                .getRequest().run();

        SelectBuilder queryBuilder = db.getQueryBuilder(GlobWithGlobType.TYPE);
        GlobAccessor globAccessor = queryBuilder.retrieve(GlobWithGlobType.VALUE);
        GlobsAccessor globsAccessor = queryBuilder.retrieve(GlobWithGlobType.VALUES);

        GlobList globs = queryBuilder.getQuery().executeAsGlobs();
        Assert.assertEquals(1, globs.size());
        Assert.assertEquals(globAccessor.getGlob(), val1);

        Assert.assertTrue(Arrays.equals(globsAccessor.getGlobs(), new Glob[]{val1, val2}));

        String s = GlobPrinter.toString(globs.get(0));
        Assert.assertEquals("id=null\n" +
                "value=[\n" +
                "  date=1\n" +
                "  value=3.14]\n" +
                "values=[  [\n" +
                "    date=1\n" +
                "    value=3.14],\n" +
                "  [\n" +
                "    date=2\n" +
                "    value=6.28]  ]\n", s);
    }
}
