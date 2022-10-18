package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.sqlstreams.SelectBuilder;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.annotations.IsBigDecimal;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.mongodb.accessor.*;
import org.globsframework.streams.accessors.*;
import org.globsframework.utils.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MongoSelectBuilder implements SelectBuilder {
    private static final Logger LOGGER = LoggerFactory.getLogger(MongoSelectBuilder.class);
    private final static MongoFieldVisitor MONGO_FIELD_VISITOR = new MongoFieldVisitor();
    private final MongoDatabase mongoDatabase;
    private final GlobType globType;
    private final MongoCollection<Document> collection;
    private final MongoDbService sqlService;
    private final Map<Field, Accessor> fieldsAndAccessor = new HashMap<>();
    private final Ref<Document> currentDoc = new Ref<>();
    private final List<Order> orders = new ArrayList<>();
    private Constraint constraint;
    private int top = -1;

    public MongoSelectBuilder(MongoDatabase mongoDatabase, GlobType globType, MongoDbService sqlService, Constraint constraint) {
        this.mongoDatabase = mongoDatabase;
        this.globType = globType;
        this.sqlService = sqlService;
        this.constraint = constraint;
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
        accessor.set(retrieve(field));
        return this;
    }

    public SelectBuilder select(LongField field, Ref<LongAccessor> accessor) {
        accessor.set(retrieve(field));
        return this;
    }

    public SelectBuilder select(BooleanField field, Ref<BooleanAccessor> accessor) {
        accessor.set(retrieve(field));
        return this;
    }

    public SelectBuilder select(StringField field, Ref<StringAccessor> accessor) {
        accessor.set(retrieve(field));
        return this;
    }

    public SelectBuilder select(DoubleField field, Ref<DoubleAccessor> accessor) {
        accessor.set(retrieve(field));
        return this;
    }

    public SelectBuilder select(DateTimeField field, Ref<DateTimeAccessor> accessor) {
        accessor.set(retrieve(field));
        return this;
    }

    public SelectBuilder select(DateField field, Ref<DateAccessor> accessor) {
        accessor.set(retrieve(field));
        return this;
    }

    public SelectBuilder select(BlobField field, Ref<BlobAccessor> accessor) {
        accessor.set(retrieve(field));
        return this;
    }

    public SelectBuilder select(GlobField field, Ref<GlobAccessor> accessor) {
        accessor.set(retrieve(field));
        return this;
    }

    public SelectBuilder select(GlobArrayField field, Ref<GlobsAccessor> accessor) {
        accessor.set(retrieve(field));
        return this;
    }

    public SelectBuilder groupBy(Field field) {
        throw new RuntimeException("Group by not implemented.");
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

    public SelectBuilder skip(int n) {
        throw new RuntimeException("Not implemented");
    }

    public SelectBuilder withKeys() {
        completeWithKeys();
        return this;
    }

    @Override
    public IntegerAccessor max(IntegerField field) {
        return null;
    }

    @Override
    public LongAccessor max(LongField field) {
        return null;
    }

    @Override
    public LongAccessor count(Field field) {
        return null;
    }

    @Override
    public SelectBuilder select(LongArrayField field, Ref<LongArrayAccessor> accessor) {
        return null;
    }

    @Override
    public IntegerArrayAccessor retrieve(IntegerArrayField field) {
        return null;
    }

    @Override
    public LongArrayAccessor retrieve(LongArrayField field) {
        return null;
    }

    @Override
    public DoubleArrayAccessor retrieve(DoubleArrayField field) {
        return null;
    }

    @Override
    public BooleanArrayAccessor retrieve(BooleanArrayField field) {
        return null;
    }

    @Override
    public LongAccessor sum(IntegerField count) {
        return null;
    }

    @Override
    public LongAccessor sum(LongField field) {
        return null;
    }

    @Override
    public IntegerAccessor min(IntegerField field) {
        return null;
    }

    @Override
    public LongAccessor min(LongField field) {
        return null;
    }

    private void completeWithKeys() {
        for (Field field : globType.getKeyFields()) {
            if (!fieldsAndAccessor.containsKey(field)) {
                select(field);
            }
        }
    }

    public IntegerAccessor retrieve(IntegerField field) {
        return (IntegerAccessor) fieldsAndAccessor.computeIfAbsent(field, f -> new IntegerMongoAccessor(sqlService.getColumnName(f), currentDoc));
    }

    public LongAccessor retrieve(LongField field) {
        return (LongAccessor) fieldsAndAccessor.computeIfAbsent(field, f -> new LongMongoAccessor(sqlService.getColumnName(f), currentDoc));
    }

    public StringAccessor retrieve(StringField field) {
        return (StringAccessor) fieldsAndAccessor.computeIfAbsent(field, f -> {
            StringAccessor stringAccessor = (StringAccessor) sqlService.getAdapter(f).getAccessor(currentDoc, sqlService);
            if (stringAccessor != null) {
                return stringAccessor;
            }
//            if (f.hasAnnotation(DbRef.KEY)) {
//                stringAccessor = new RefStringMongoAccessor(sqlService.getFirstLevelColumnName(f), currentDoc);
//            } else if (f.hasAnnotation(IsDbKey.KEY) || f.isKeyField() && f.getGlobType().getKeyFields().length == 1) {
//                stringAccessor = new KeyStringMongoAccessor(sqlService.getColumnName(f), currentDoc);
//            }  else {
            stringAccessor = new StringMongoAccessor(sqlService.getColumnName(f), currentDoc);
//            }
            return stringAccessor;
        });
    }

    public BooleanAccessor retrieve(BooleanField field) {
        return (BooleanAccessor) fieldsAndAccessor.computeIfAbsent(field, f -> new BooleanMongoAccessor(sqlService.getColumnName(f), currentDoc));
    }

    public DoubleAccessor retrieve(DoubleField field) {
        return (DoubleAccessor) fieldsAndAccessor.computeIfAbsent(field, f -> {
            DoubleAccessor doubleAccessor;
            if (f.hasAnnotation(IsBigDecimal.KEY)) {
                doubleAccessor = new DoubleFromBigDecimalMongoAccessor(sqlService.getColumnName(f), currentDoc);
            } else {
                doubleAccessor = new DoubleMongoAccessor(sqlService.getColumnName(f), currentDoc);
            }
            return doubleAccessor;
        });
    }

    public DateTimeAccessor retrieve(DateTimeField field) {
        throw new RuntimeException("Not implemented");
    }

    public DateAccessor retrieve(DateField field) {
        throw new RuntimeException("Not implemented");
    }

    public BlobAccessor retrieve(BlobField field) {
        return (BlobAccessor) fieldsAndAccessor.computeIfAbsent(field, (f) -> new BlobMongoAccessor(sqlService.getColumnName(f), currentDoc));
    }

    public StringArrayAccessor retrieve(StringArrayField field) {
        return (StringArrayAccessor) fieldsAndAccessor.computeIfAbsent(field, f -> new StringArrayMongoAccessor(sqlService.getColumnName(f), currentDoc));
    }

    public Accessor retrieveUnTyped(Field field) {
        return fieldsAndAccessor.computeIfAbsent(field, (f) -> field.safeVisit(MONGO_FIELD_VISITOR, this).accessor);
    }

    public GlobAccessor retrieve(GlobField field) {
        return (GlobAccessor) fieldsAndAccessor.computeIfAbsent(field, (f) -> new GlobMongoAccessor(field, currentDoc, sqlService));
    }

    public GlobAccessor retrieve(GlobUnionField field) {
        throw new RuntimeException("Not implemented");
    }

    public GlobsAccessor retrieve(GlobArrayField field) {
        return (GlobsAccessor) fieldsAndAccessor.computeIfAbsent(field, (f) -> new GlobsMongoAccessor(field, currentDoc, sqlService));
    }

    public GlobsAccessor retrieve(GlobArrayUnionField field) {
        return null;
    }

    static class Order {
        public final Field field;
        public final boolean asc;

        public Order(Field field, boolean asc) {
            this.field = field;
            this.asc = asc;
        }
    }

    static class MongoFieldVisitor extends FieldVisitorWithContext.AbstractWithErrorVisitor<MongoSelectBuilder> {
        Accessor accessor;

        public void visitInteger(IntegerField field, MongoSelectBuilder builder) {
            accessor = builder.retrieve(field);
        }

        public void visitDouble(DoubleField field, MongoSelectBuilder builder) {
            accessor = builder.retrieve(field);
        }

        public void visitString(StringField field, MongoSelectBuilder builder) {
            accessor = builder.retrieve(field);
        }

        public void visitBoolean(BooleanField field, MongoSelectBuilder builder) {
            accessor = builder.retrieve(field);
        }

        public void visitLong(LongField field, MongoSelectBuilder builder) {
            accessor = builder.retrieve(field);
        }

        public void visitBlob(BlobField field, MongoSelectBuilder builder) {
            accessor = builder.retrieve(field);
        }

        public void visitGlob(GlobField field, MongoSelectBuilder builder) {
            accessor = builder.retrieve(field);
        }

        public void visitGlobArray(GlobArrayField field, MongoSelectBuilder builder) {
            accessor = builder.retrieve(field);
        }
    }
}
