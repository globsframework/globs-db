package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.BulkDbRequest;
import org.globsframework.sqlstreams.SqlRequest;
import org.globsframework.sqlstreams.annotations.IsDbKey;
import org.globsframework.sqlstreams.constraints.Constraint;
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

class MongoUpdateSqlRequest implements BulkDbRequest {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoUpdateSqlRequest.class);
    public static final int BATCH_SIZE = Integer.getInteger("mongo.update.batch.size", 500);
    public static final int MAX_PENDING = Integer.getInteger("mongo.update.pending.max", 10);
    private final MongoCollection<Document> collection;
    private MongoDbService sqlService;
    private final Constraint constraint;
    private final Pair<MongoDbService.UpdateAdapter, Accessor> fieldsValues[];
    private boolean bulk;
    private List<UpdateOneModel<Document>> docs;
    CompletableFuture<Boolean> completableFuture;
    private int count = 0;
    ThreadUtils.Limiter limiter = ThreadUtils.createLimiter(MAX_PENDING);
    private UpdateOptions upsert;

    public MongoUpdateSqlRequest(MongoDbService sqlService, MongoDatabase mongoDatabase, GlobType globType, Constraint constraint,
                                 Map<Field, Accessor> fieldsValues, boolean bulk) {
        this.sqlService = sqlService;
        this.constraint = constraint;
        this.bulk = bulk;
        this.fieldsValues = fieldsValues.entrySet().stream()
                .filter(f -> !f.getKey().hasAnnotation(IsDbKey.KEY))
                .map(e -> Pair.makePair(sqlService.getAdapter(e.getKey()), e.getValue()))
                .toArray(Pair[]::new);
        collection = mongoDatabase.getCollection(sqlService.getTableName(globType));
        upsert = new UpdateOptions().upsert(true);
    }

    public void run() throws SqlException {
        Bson filter;
        if (constraint != null) {
            MongoConstraintVisitor constraintVisitor = new MongoConstraintVisitor(sqlService);
            constraint.visit(constraintVisitor);
            filter = constraintVisitor.filter;
        } else {
            filter = new Document();
        }
        List<Bson> updates = new ArrayList<>(fieldsValues.length);
        for (Pair<MongoDbService.UpdateAdapter, Accessor> fieldsValue : fieldsValues) {
            Object objectValue = fieldsValue.getSecond().getObjectValue();
            if (objectValue != null) {
                updates.add(fieldsValue.getFirst().update(objectValue));
            }
        }
        Bson combine = Updates.combine(updates);

        LOGGER.debug("Update with filter {} on {}", filter, combine);
        if (++count <= 2 && !bulk) {
            collection.updateOne(filter, combine, upsert);
        } else {
            UpdateOneModel<Document> updateOneModel = new UpdateOneModel<Document>(filter, combine, upsert);
            if (docs == null) {
                docs = new ArrayList<>(BATCH_SIZE);
            }
            docs.add(updateOneModel);
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
                    final List<UpdateOneModel<Document>> toInsert = docs;
                    docs = null;
                    completableFuture = completableFuture.
                            thenApplyAsync(ok -> {
                                try {
                                    collection.bulkWrite(toInsert);
                                } finally {
                                    limiter.notifyDown();
                                }
                                return true;
                            }, sqlService.getExecutor());
                } else {
                    final List<UpdateOneModel<Document>> toInsert = docs;
                    docs = null;
                    completableFuture = CompletableFuture.supplyAsync(() -> {
                        try {
                            collection.bulkWrite(toInsert);
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
        LOGGER.info(count + " doc updated.");
    }

    private void flushAll() {
        if (completableFuture != null) {
            if (docs != null && !docs.isEmpty()) {
                completableFuture = completableFuture.thenApply(ok -> {
                    collection.bulkWrite(docs);
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
                collection.bulkWrite(docs);
            }
        }
        docs = null;
    }

    public void flush() {
        flushAll();
    }
}
