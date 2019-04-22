package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.globsframework.metamodel.Field;
import org.globsframework.sqlstreams.BulkDbRequest;
import org.globsframework.sqlstreams.exceptions.SqlException;
import org.globsframework.sqlstreams.utils.ThreadUtils;
import org.globsframework.streams.accessors.Accessor;
import org.globsframework.utils.collections.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

class MongoCreateSqlRequest implements BulkDbRequest {
    private static Logger LOGGER = LoggerFactory.getLogger(MongoCreateSqlRequest.class);
    public static final int BATCH_SIZE = Integer.getInteger("mongo.create.batch.size", 500);
    public static final int MAX_PENDING = Integer.getInteger("mongo.create.pending.max", 10);
    private MongoCollection<Document> collection;
    private final Pair<MongoDbService.UpdateAdapter, Accessor> fieldsValues[];
    private MongoDbService sqlService;
    private boolean bulk;
    private List<Document> docs;
    CompletableFuture<Boolean> completableFuture;
    ThreadUtils.Limiter limiter = ThreadUtils.createLimiter(MAX_PENDING);
    private int count = 0;

    public MongoCreateSqlRequest(MongoCollection<Document> collection,
                                 Map<Field, Accessor> fieldsValues, MongoDbService sqlService, boolean bulk) {
        this.collection = collection;
        this.fieldsValues = fieldsValues.entrySet().stream()
                .map(e -> Pair.makePair(sqlService.getAdapter(e.getKey()), e.getValue()))
                .toArray(Pair[]::new);
        this.sqlService = sqlService;
        this.bulk = bulk;
    }

    public void run() throws SqlException {
        Document doc = new Document();
        for (Pair<MongoDbService.UpdateAdapter, Accessor> fieldAccessorEntry : fieldsValues) {
            Object objectValue = fieldAccessorEntry.getSecond().getObjectValue();
            MongoDbService.UpdateAdapter first = fieldAccessorEntry.getFirst();
            if (objectValue != null) {
                first.create(objectValue, doc, sqlService);
            }
        }

        LOGGER.debug("create {}", doc);
        if (++count <= 2 && !bulk) {
            collection.insertOne(doc);
        } else {
            if (docs == null) {
                docs = new ArrayList<>(BATCH_SIZE);
            }
            docs.add(doc);
            if (docs.size() == BATCH_SIZE) {
                limiter.limitParallelConnection();
                if (completableFuture != null) {
                    if (completableFuture.isCompletedExceptionally()) {
                        try {
                            completableFuture.get();
                        } catch (Exception e) {
                            throw new RuntimeException("Create failed ", e);
                        }
                    }
                    final List<Document> toInsert = docs;
                    docs = null;
                    completableFuture = completableFuture.
                          thenApplyAsync(ok -> {
                              try {
                                  collection.insertMany(toInsert);
                              } finally {
                                  limiter.notifyDown();
                              }
                              return true;
                          }, sqlService.getExecutor());
                } else {
                    final List<Document> toInsert = docs;
                    docs = null;
                    completableFuture = CompletableFuture.supplyAsync(() -> {
                        try {
                            collection.insertMany(toInsert);
                        } finally {
                            limiter.notifyDown();
                        }
                        return Boolean.TRUE;
                    }, sqlService.getExecutor());
                }
            }
        }
    }

    public void close() {
        flushAll();
        LOGGER.info(count + " doc inserted");
    }

    private void flushAll() {
        if (completableFuture != null) {
            if (docs != null && !docs.isEmpty()) {
                completableFuture = completableFuture.thenApply(ok -> {
                    collection.insertMany(docs);
                    return true;
                });
            }
            try {
                completableFuture.get(1, TimeUnit.MINUTES);
                completableFuture = null;
            } catch (Exception e) {
                completableFuture = null;
                throw new RuntimeException("In close, fail to insert all data", e);
            }
        }
        else {
            if (docs != null && !docs.isEmpty()) {
                collection.insertMany(docs);
            }
        }
        docs = null;
    }

    public void flush() {
        flushAll();
    }
}
