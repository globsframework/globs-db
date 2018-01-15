package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import org.bson.Document;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.sqlstreams.SelectBuilder;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.annotations.DbRef;
import org.globsframework.sqlstreams.annotations.IsBigDecimal;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.mongodb.accessor.*;
import org.globsframework.streams.accessors.*;
import org.globsframework.utils.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class MongoSelectBuilder implements SelectBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoSelectBuilder.class);
    private final static MongoFieldVisitor MONGO_FIELD_VISITOR = new MongoFieldVisitor();
    private final MongoDatabase mongoDatabase;
    private final GlobType globType;
    private final MongoCollection<Document> collection;
    private final MongoDbService sqlService;
    private Constraint constraint;
    private final Map<Field, Accessor> fieldsAndAccessor = new HashMap<>();
    private final Ref<Document> currentDoc = new Ref<>();
    private final List<Order> orders = new ArrayList<>();
    private int top = - 1;

    static class Order {
        public final Field field;
        public final boolean asc;

        public Order(Field field, boolean asc) {
            this.field = field;
            this.asc = asc;
        }
    }

    public MongoSelectBuilder(MongoDatabase mongoDatabase, GlobType globType, MongoDbService sqlService, Constraint constraint) {
        this.mongoDatabase = mongoDatabase;
        this.globType = globType;
        this.sqlService = sqlService;
        this.constraint = constraint;
//        if (LOGGER.isDebugEnabled()) {
            CompletableFuture completableFuture = new CompletableFuture();
            mongoDatabase.listCollections().forEach(document -> LOGGER.debug(document.toJson()), (result, t) -> {
                completableFuture.complete(null);
            });
            try {
                completableFuture.get();
            } catch (Exception e) {
                throw new RuntimeException("Fail to connect to db ");
            }
//        }
        collection = mongoDatabase.getCollection(sqlService.getTableName(globType), Document.class);
    }

    public SelectQuery getQuery() {
        return new MongoSelectQuery(collection, fieldsAndAccessor, currentDoc, globType, sqlService, constraint, orders, top);
    }

    public SelectQuery getNotAutoCloseQuery() {
        return getQuery();
    }

    public SelectBuilder select(Field field) {
        field.safeVisit(MONGO_FIELD_VISITOR, this);
        return this;
    }

    public SelectBuilder selectAll() {
        globType.streamFields().forEach(this::select);
        return this;
    }

    public SelectBuilder select(IntegerField field, Ref<IntegerAccessor> accessor) {
        accessor.set(new IntegerMongoAccessor(sqlService.getColumnName(field), currentDoc));
        fieldsAndAccessor.put(field, accessor.get());
        return this;
    }

    public SelectBuilder select(LongField field, Ref<LongAccessor> accessor) {
        accessor.set(new LongMongoAccessor(sqlService.getColumnName(field), currentDoc));
        fieldsAndAccessor.put(field, accessor.get());
        return this;

    }

    public SelectBuilder select(BooleanField field, Ref<BooleanAccessor> accessor) {
        accessor.set(new BooleanMongoAccessor(sqlService.getColumnName(field), currentDoc));
        fieldsAndAccessor.put(field, accessor.get());
        return this;
    }

    public SelectBuilder select(StringField field, Ref<StringAccessor> accessor) {
        if (field.hasAnnotation(DbRef.KEY)) {
            accessor.set(new RefStringMongoAccessor(sqlService.getFirstLevelColumnName(field), currentDoc));
        } else if (field.isKeyField() && field.getGlobType().getKeyFields().length == 1) {
            accessor.set(new KeyStringMongoAccessor(sqlService.getColumnName(field), currentDoc));
        } else {
            accessor.set(new StringMongoAccessor(sqlService.getColumnName(field), currentDoc));
        }
        fieldsAndAccessor.put(field, accessor.get());
        return this;
    }

    public SelectBuilder select(DoubleField field, Ref<DoubleAccessor> accessor) {
        accessor.set(new DoubleMongoAccessor(sqlService.getColumnName(field), currentDoc));
        fieldsAndAccessor.put(field, accessor.get());
        return this;
    }

    public SelectBuilder select(BlobField field, Ref<BlobAccessor> accessor) {
        accessor.set(new BlobMongoAccessor(sqlService.getColumnName(field), currentDoc));
        fieldsAndAccessor.put(field, accessor.get());
        return this;
    }

    public SelectBuilder orderAsc(Field field) {
        orders.add(new Order(field, true));
        return this;
    }

    public SelectBuilder orderDesc(Field field) {
        orders.add(new Order(field, false));
        return this;
    }

    public SelectBuilder top(int n) {
        top = n;
        return this;
    }

    public SelectBuilder withKeys() {
        completeWithKeys();
        return this;
    }

    private void completeWithKeys() {
        for (Field field : globType.getKeyFields()) {
            if (!fieldsAndAccessor.containsKey(field)) {
                select(field);
            }
        }
    }

    public IntegerAccessor retrieve(IntegerField field) {
        IntegerMongoAccessor longMongoAccessor = new IntegerMongoAccessor(sqlService.getColumnName(field), currentDoc);
        fieldsAndAccessor.put(field, longMongoAccessor);
        return longMongoAccessor;
    }

    public LongAccessor retrieve(LongField field) {
        LongMongoAccessor longMongoAccessor = new LongMongoAccessor(sqlService.getColumnName(field), currentDoc);
        fieldsAndAccessor.put(field, longMongoAccessor);
        return longMongoAccessor;
    }

    public StringAccessor retrieve(StringField field) {
        StringAccessor stringAccessor;
        if (field.hasAnnotation(DbRef.KEY)) {
            stringAccessor = new RefStringMongoAccessor(sqlService.getFirstLevelColumnName(field), currentDoc);
        } else if (field.isKeyField() && field.getGlobType().getKeyFields().length == 1) {
            stringAccessor = new KeyStringMongoAccessor(sqlService.getColumnName(field), currentDoc);
        } else if (field.getName().equals("_id") || field.getName().equals("_uuid"))  {                //  TODO TBR
            stringAccessor = new KeyStringMongoAccessor(sqlService.getColumnName(field), currentDoc);
        }
        else {
            stringAccessor = new StringMongoAccessor(sqlService.getColumnName(field), currentDoc);
        }
        fieldsAndAccessor.put(field, stringAccessor);
        return stringAccessor;
    }

    public BooleanAccessor retrieve(BooleanField field) {
        BooleanMongoAccessor longMongoAccessor = new BooleanMongoAccessor(sqlService.getColumnName(field), currentDoc);
        fieldsAndAccessor.put(field, longMongoAccessor);
        return longMongoAccessor;
    }

    public DoubleAccessor retrieve(DoubleField field) {
        DoubleAccessor doubleAccessor;
        if (field.hasAnnotation(IsBigDecimal.KEY)) {
            doubleAccessor = new DoubleFromBigDecimalMongoAccessor(sqlService.getColumnName(field), currentDoc);
        } else {
            doubleAccessor = new DoubleMongoAccessor(sqlService.getColumnName(field), currentDoc);
        }
        fieldsAndAccessor.put(field, doubleAccessor);
        return doubleAccessor;
    }

    public BlobAccessor retrieve(BlobField field) {
        BlobMongoAccessor longMongoAccessor = new BlobMongoAccessor(sqlService.getColumnName(field), currentDoc);
        fieldsAndAccessor.put(field, longMongoAccessor);
        return longMongoAccessor;
    }


    public Accessor retrieveUnTyped(Field field) {
        return field.safeVisit(MONGO_FIELD_VISITOR, this).accessor;
    }

    static class MongoFieldVisitor implements FieldVisitorWithContext<MongoSelectBuilder> {
        Accessor accessor;
        public void visitInteger(IntegerField field, MongoSelectBuilder builder) throws Exception {
            accessor = builder.retrieve(field);
        }

        public void visitDouble(DoubleField field, MongoSelectBuilder builder) throws Exception {
            accessor = builder.retrieve(field);
        }

        public void visitString(StringField field, MongoSelectBuilder builder) throws Exception {
            accessor = builder.retrieve(field);
        }

        public void visitBoolean(BooleanField field, MongoSelectBuilder builder) throws Exception {
            accessor = builder.retrieve(field);
        }

        public void visitLong(LongField field, MongoSelectBuilder builder) throws Exception {
            accessor = builder.retrieve(field);
        }

        public void visitBlob(BlobField field, MongoSelectBuilder builder) throws Exception {
            accessor = builder.retrieve(field);
        }
    }


}
