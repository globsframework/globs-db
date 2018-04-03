package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.async.client.MongoCollection;
import com.mongodb.async.client.MongoDatabase;
import org.bson.Document;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.sqlstreams.SelectBuilder;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.drivers.mongodb.accessor.*;
import org.globsframework.streams.accessors.*;
import org.globsframework.utils.Ref;

import java.util.HashMap;
import java.util.Map;

public class MongoSelectBuilder implements SelectBuilder {
    private final static MongoFieldVisitor MONGO_FIELD_VISITOR = new MongoFieldVisitor();
    private final MongoDatabase mongoDatabase;
    private final GlobType globType;
    private final MongoCollection<Document> collection;
    private final SqlService sqlService;
    private final Map<Field, Accessor> fieldsAndAccessor = new HashMap<>();
    private final Ref<Document> currentDoc = new Ref<>();

    public MongoSelectBuilder(MongoDatabase mongoDatabase, GlobType globType, SqlService sqlService) {
        this.mongoDatabase = mongoDatabase;
        this.globType = globType;
        this.sqlService = sqlService;
        collection = mongoDatabase.getCollection(sqlService.getTableName(globType), Document.class);
    }

    public SelectQuery getQuery() {
        return new MongoSelectQuery(collection, fieldsAndAccessor, currentDoc, globType, sqlService);
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
        accessor.set(new StringMongoAccessor(sqlService.getColumnName(field), currentDoc));
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
        StringMongoAccessor longMongoAccessor = new StringMongoAccessor(sqlService.getColumnName(field), currentDoc);
        fieldsAndAccessor.put(field, longMongoAccessor);
        return longMongoAccessor;
    }

    public BooleanAccessor retrieve(BooleanField field) {
        BooleanMongoAccessor longMongoAccessor = new BooleanMongoAccessor(sqlService.getColumnName(field), currentDoc);
        fieldsAndAccessor.put(field, longMongoAccessor);
        return longMongoAccessor;
    }

    public DoubleAccessor retrieve(DoubleField field) {
        DoubleMongoAccessor longMongoAccessor = new DoubleMongoAccessor(sqlService.getColumnName(field), currentDoc);
        fieldsAndAccessor.put(field, longMongoAccessor);
        return longMongoAccessor;
    }

    public BlobAccessor retrieve(BlobField field) {
        BlobMongoAccessor longMongoAccessor = new BlobMongoAccessor(sqlService.getColumnName(field), currentDoc);
        fieldsAndAccessor.put(field, longMongoAccessor);
        return longMongoAccessor;
    }


    public Accessor retrieveUnTyped(Field field) {
        throw new RuntimeException("TODO : Not implemented");
    }

    static class MongoFieldVisitor implements FieldVisitorWithContext<MongoSelectBuilder> {

        public void visitInteger(IntegerField field, MongoSelectBuilder builder) throws Exception {
            builder.retrieve(field);
        }

        public void visitDouble(DoubleField field, MongoSelectBuilder builder) throws Exception {
            builder.retrieve(field);
        }

        public void visitString(StringField field, MongoSelectBuilder builder) throws Exception {
            builder.retrieve(field);
        }

        public void visitBoolean(BooleanField field, MongoSelectBuilder builder) throws Exception {
            builder.retrieve(field);
        }

        public void visitLong(LongField field, MongoSelectBuilder builder) throws Exception {
            builder.retrieve(field);
        }

        public void visitBlob(BlobField field, MongoSelectBuilder builder) throws Exception {
            builder.retrieve(field);
        }
    }


}
