package org.globsframework.sqlstreams.drivers.mongodb;

import com.github.fakemongo.junit.FongoAsyncRule;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import org.bson.Document;
import org.bson.codecs.Codec;
import org.bson.codecs.configuration.CodecProvider;
import org.bson.codecs.configuration.CodecRegistries;
import org.bson.codecs.configuration.CodecRegistry;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoader;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.KeyField;
import org.globsframework.metamodel.fields.*;
import org.globsframework.metamodel.index.MultiFieldUniqueIndex;
import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.model.KeyBuilder;
import org.globsframework.model.MutableGlob;
import org.globsframework.model.impl.DefaultGlob;
import org.globsframework.model.repository.DefaultGlobRepository;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.globsframework.sqlstreams.drivers.mongodb.MongoSelectTest.DummyObject.*;

public class MongoSelectTest {
    @Rule
    public FongoAsyncRule fongoAsyncRule = new FongoAsyncRule();

    @Test
    public void Select() throws Exception {
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
    public void IsNullIsExist() throws ExecutionException, InterruptedException {
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
    public void contains() throws ExecutionException, InterruptedException {
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
    public void notIn() throws ExecutionException, InterruptedException {
        InitDb initDb = new InitDb().invoke();
        MongoDatabase database = initDb.getDatabase();
        MongoDbService sqlService = initDb.getSqlService();

        SqlConnection mangoDbConnection = new MongoDbConnection(database, sqlService);
        GlobList globs = mangoDbConnection.getQueryBuilder(DummyObject.TYPE, Constraints.notIn(DummyObject.NAME, Arrays.asList("name 1", "name 2")))
              .selectAll()
              .getQuery()
              .executeAsGlobs();
        Assert.assertEquals(2, globs.size());
        Assert.assertTrue(globs.stream()
              .map(g -> g.get(DummyObject.NAME))
              .anyMatch(s -> s.equals("name 3")));
    }

    @Test
    public void orderAndLimit() throws ExecutionException, InterruptedException {
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
    public void testInOp() throws ExecutionException, InterruptedException {
        InitDb initDb = new InitDb().invoke();
        MongoDatabase database = initDb.getDatabase();
        MongoDbService sqlService = initDb.getSqlService();
        SqlConnection mangoDbConnection = new MongoDbConnection(database, sqlService);

        GlobList sortedFirstGlob = mangoDbConnection.getQueryBuilder(DummyObject.TYPE, Constraints.in(DummyObject.NAME, Arrays.asList("name 1", "name 3")))
              .selectAll()
              .orderAsc(DummyObject.ID)
              .getQuery().executeAsGlobs();

        Assert.assertEquals(2, sortedFirstGlob.size());
        Assert.assertEquals(1, sortedFirstGlob.get(0).get(ID).intValue());
        Assert.assertEquals(3, sortedFirstGlob.get(1).get(ID).intValue());
    }

    private void insert(MongoCollection<Glob> globMongoCollection, MutableGlob data, MongoDbService sqlService) throws InterruptedException, java.util.concurrent.ExecutionException {
        CompletableFuture waitOn = new CompletableFuture();

        globMongoCollection.insertOne(data, (aVoid, throwable) -> {
            if (throwable != null) {
                throwable.printStackTrace();
            }
            waitOn.complete(null);
        });
        waitOn.get();
    }

    @Test
    public void checkIndexCreation() throws Exception {
        MongoDatabase database = fongoAsyncRule.getDatabase();
        MongoCollection<Document> globMongoCollection = database.getCollection(DummyObject.TYPE.getName(), Document.class);
        MongoUtils.createIndexIfNeeded(globMongoCollection, Collections.singleton(NAME_INDEX));
        CompletableFuture<Boolean> future = new CompletableFuture<>();
        List<Document> index = new ArrayList<>();
        globMongoCollection.listIndexes().into(index, (documents, throwable) -> {
            for (Document document : documents) {
                if (MongoUtils.contain(NAME_INDEX, document)){
                    future.complete(true);
                    return;
                }
            }
            future.complete(false);
        });
        Assert.assertTrue(future.get());
    }

    static public class DummyObject {
        public static GlobType TYPE;

        @KeyField
        public static IntegerField ID;

        public static DoubleField VALUE;

        public static StringField NAME;

        public static StringField NAME_2;

        public static MultiFieldUniqueIndex NAME_INDEX;

        static {
            GlobTypeLoader globTypeLoader = GlobTypeLoaderFactory.create(DummyObject.class);
            globTypeLoader.load();
            globTypeLoader.defineMultiFieldUniqueIndex(NAME_INDEX, NAME, NAME_2);
        }
    }


    private class InitDb {
        private MongoDatabase database;
        private MongoDbService sqlService;

        public MongoDatabase getDatabase() {
            return database;
        }

        public MongoDbService getSqlService() {
            return sqlService;
        }

        public InitDb invoke() throws InterruptedException, java.util.concurrent.ExecutionException {
            database = fongoAsyncRule.getDatabase();
            sqlService = new MongoDbService(database);

            MongoCollection<Glob> globMongoCollection = database
                  .getCollection(sqlService.getTableName(DummyObject.TYPE), Glob.class)
                  .withCodecRegistry(CodecRegistries.fromProviders(new CodecProvider() {
                      public <T> Codec<T> get(Class<T> aClass, CodecRegistry codecRegistry) {
                          if (aClass == DefaultGlob.class || aClass == Glob.class) {
                              return (Codec<T>) new GlobCodec(DummyObject.TYPE, sqlService);
                          }
                          return null;
                      }
                  }));

            insert(globMongoCollection, DummyObject.TYPE.instantiate()
                  .set(DummyObject.ID, 1)
                  .set(DummyObject.NAME, "name 1")
                  .set(DummyObject.NAME_2, "second name")
                  .set(VALUE, 3.14), sqlService);
            insert(globMongoCollection, DummyObject.TYPE.instantiate()
                  .set(DummyObject.ID, 2)
                  .set(DummyObject.NAME, "name 2")
                  .set(VALUE, 3.14 * 2.), sqlService);
            insert(globMongoCollection, DummyObject.TYPE.instantiate()
                  .set(DummyObject.ID, 3)
                  .set(DummyObject.NAME, "name 3")
                  .set(VALUE, 3.14 * 3.), sqlService);
            insert(globMongoCollection, DummyObject.TYPE.instantiate()
                  .set(DummyObject.ID, 4)
                  .set(DummyObject.NAME, "my name")
                  .set(VALUE, 3.14 * 3.), sqlService);
            return this;
        }
    }
}
