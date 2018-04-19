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
import java.util.concurrent.CompletionService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.Future;

class MongoUpdateSqlRequestParallele implements BulkDbRequest {
    public static final int BATCH_SIZE = Integer.getInteger("mongo.update.batch.size", 500);
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoUpdateSqlRequestParallele.class);
    private final MongoCollection<Document> collection;
    private MongoDbService sqlService;
    private final Constraint constraint;
    private final Pair<MongoDbService.UpdateAdapter, Accessor> fieldsValues[];
    private boolean bulk;
    private List<UpdateOneModel<Document>> docs;
    private int count = 0;
    private UpdateOptions upsert;
    private CompletionService<Boolean> completionService;
    ThreadUtils.Limiter limiter = ThreadUtils.createLimiter(10);

    public MongoUpdateSqlRequestParallele(MongoDbService sqlService, MongoDatabase mongoDatabase, GlobType globType, Constraint constraint,
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
        completionService = new ExecutorCompletionService<>(sqlService.getExecutor());
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
                final List<UpdateOneModel<Document>> toInsert = docs;
                docs = null;
                completionService.submit(() -> {
                    try {
                        collection.bulkWrite(toInsert);
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
        LOGGER.info(count + " doc updated.");
    }

    private void flushAll() {
        if (docs != null && !docs.isEmpty()) {
            collection.bulkWrite(docs);
        }
        docs = null;
        limiter.waitAllDone();
        readAllComplete();
    }

    public void flush() {
        flushAll();
    }

    private void readAllComplete() {
        try {
            while (true) {
                Future<Boolean> poll = completionService.poll();
                if (poll != null) {
                    poll.get();
                } else {
                    return;
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
