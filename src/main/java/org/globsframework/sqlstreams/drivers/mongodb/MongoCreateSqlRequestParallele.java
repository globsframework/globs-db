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
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

class MongoCreateSqlRequestParallele implements BulkDbRequest {
    public static final int BATCH_SIZE = Integer.getInteger("mongo.create.batch.size", 500);
    private static Logger LOGGER = LoggerFactory.getLogger(MongoCreateSqlRequestParallele.class);
    private MongoCollection<Document> collection;
    private final Pair<MongoDbService.UpdateAdapter, Accessor> fieldsValues[];
    private MongoDbService sqlService;
    private boolean bulk;
    private List<Document> docs;
    private int count = 0;
    private CompletionService<Boolean> completionService;
    ThreadUtils.Limiter limiter = ThreadUtils.createLimiter(10);

    public MongoCreateSqlRequestParallele(MongoCollection<Document> collection,
                                          Map<Field, Accessor> fieldsValues, MongoDbService sqlService, boolean bulk) {
        this.collection = collection;
        this.fieldsValues = fieldsValues.entrySet().stream()
                .map(e -> Pair.makePair(sqlService.getAdapter(e.getKey()), e.getValue()))
                .toArray(Pair[]::new);
        this.sqlService = sqlService;
        this.bulk = bulk;
        completionService = new ExecutorCompletionService<>(sqlService.getExecutor());
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
                final List<Document> toInsert = docs;
                docs = null;
                completionService.submit(() -> {
                    try {
                        collection.insertMany(toInsert);
                    } finally {
                        limiter.notifyDown();
                    }
                    return true;
                });
            }
        }
    }

    public void close() {
        flushAll();
        LOGGER.info(count + " doc inserted");
    }

    private void flushAll() {
        if (docs != null && !docs.isEmpty()) {
            collection.insertMany(docs);
        }
        docs = null;
        limiter.waitAllDone();
        readAllComplete();
    }

    private void readAllComplete()  {
        try {
            while (true) {
                Future<Boolean> poll = completionService.poll();
                if (poll != null) {
                    poll.get();
                }
                else {
                    return;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public void flush() {
        flushAll();
    }
}
