package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.async.client.ListIndexesIterable;
import com.mongodb.async.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.metamodel.index.Index;
import org.globsframework.metamodel.index.impl.IsUniqueIndexVisitor;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.CreateBuilder;
import org.globsframework.sqlstreams.SqlRequest;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.annotations.DbFieldName;
import org.globsframework.sqlstreams.annotations.DbRef;
import org.globsframework.sqlstreams.annotations.IsDbKey;
import org.globsframework.streams.accessors.*;
import org.globsframework.utils.Ref;
import org.globsframework.utils.collections.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.CompletableFuture;

public class MongoUtils {
    public static final String DB_REF_ID_EXT = "id";
    public static final String DB_REF_REF_EXT = "ref";
    public static final String ID_FIELD_NAME = "_id";
    private static Logger LOGGER = LoggerFactory.getLogger(MongoUtils.class);

    public static void createIndexIfNeeded(MongoCollection<?> collection, Collection<Index> indices) {
        if (indices.isEmpty()) {
            return;
        }
        List<Document> documents = new ArrayList<>();
        ListIndexesIterable<Document> documentListIndexesIterable = collection.listIndexes();
        CompletableFuture<Throwable> future = new CompletableFuture<>();
        documentListIndexesIterable.into(documents, (result, t) -> {
            for (Index index : indices) {
                findOrCreateIndex(collection, index, documents);
            }
            future.complete(t);
        });
        Throwable throwable = null;
        try {
            throwable = future.get();
        } catch (Exception e) {
            LOGGER.error("timeout", e);
        }
        if (throwable != null) {
            LOGGER.error("while creating index ", throwable);
        }
    }

    private static void findOrCreateIndex(MongoCollection<?> collection, Index functionalIndex, List<Document> documents) {
        for (Document document : documents) {
            if (contain(functionalIndex, document)) {
                return;
            }
        }
        Document document = new Document();
        functionalIndex.fields().forEach(field -> document.append(getFullDbName(field), 1));
        LOGGER.info("create index " + functionalIndex.getName()+ " =>" + document);
        collection.createIndex(document, new IndexOptions()
              .unique(functionalIndex.visit(new IsUniqueIndexVisitor()).isUnique())
              .name(functionalIndex.getName()), (result, t) -> {
            if (t != null) {
                LOGGER.error("Fail to create index " + functionalIndex.getName(), t);
            }
        });
    }

    protected static boolean contain(Index functionalIndex, Document document) {
        Document key = document.get("key", Document.class);
        String name = document.getString("name");
        return name.equals(functionalIndex.getName()) && functionalIndex.fields()
              .allMatch(field -> key.containsKey(getFullDbName(field)));
    }

    public static String getDbName(Field field) {
        Glob name = field.findAnnotation(DbFieldName.KEY);
        if (name != null) {
            return name.get(DbFieldName.NAME);
        }
        if (field.isKeyField() && field.hasAnnotation(IsDbKey.KEY)) {
            return ID_FIELD_NAME;
        }
        return field.getName();
    }

    public static String getFullDbName(Field field) {
        String dbName = getDbName(field);
        if (field.hasAnnotation(DbRef.KEY)) {
            return dbName + '.' + DB_REF_ID_EXT;
        } else {
            return dbName;
        }
    }

    public static String getIdFromRef(Document unNormalized, String idField) {
        Document document = unNormalized.get(idField, Document.class);
        if (document == null) {
            return null;
        }
        return document.getObjectId(MongoUtils.DB_REF_ID_EXT).toString();
    }

    public static void fill(List<Glob> data, SqlService sqlService) {
        MultiMap<GlobType, Glob>  dataByType = new MultiMap<>();
        data.forEach(glob -> dataByType.put(glob.getType(), glob));
        for (Map.Entry<GlobType, List<Glob>> globTypeListEntry : dataByType.entries()) {
            Ref<Glob> ref = new Ref<>();
            GlobType globType = globTypeListEntry.getKey();
            CreateBuilder createBuilder = sqlService.getDb().getCreateBuilder(globType);
            for (Field field : globType.getFields()) {
                Accessor accessor = field.safeVisit(new FieldVisitor() {
                    Accessor accessor;

                    public void visitInteger(IntegerField field) throws Exception {
                        accessor = new IntegerGlobAccessor(field, ref);
                    }

                    public void visitDouble(DoubleField field) throws Exception {
                        accessor = new DoubleGlobAccessor(field, ref);
                    }

                    public void visitString(StringField field) throws Exception {
                        accessor = new StringGlobAccessor(field, ref);
                    }

                    public void visitBoolean(BooleanField field) throws Exception {
                        accessor = new BooleanGlobAccessor(field, ref);
                    }

                    public void visitLong(LongField field) throws Exception {
                        accessor = new LongGlobAccessor(field, ref);
                    }

                    public void visitBlob(BlobField field) throws Exception {

                    }
                }).accessor;
                createBuilder.setObject(field, accessor);
            }
            SqlRequest request = createBuilder.getRequest();
            for (Glob glob : globTypeListEntry.getValue()) {
                ref.set(glob);
                request.run();
            }
            request.close();
        }
    }


    static class DoubleGlobAccessor implements DoubleAccessor{
        private final DoubleField field;
        private final Ref<Glob> glob;

        DoubleGlobAccessor(DoubleField field, Ref<Glob> glob) {
            this.field = field;
            this.glob = glob;
        }

        public Double getDouble() {
            return glob.get().get(field);
        }

        public double getValue(double valueIfNull) {
            return glob.get().get(field, valueIfNull);
        }

        public boolean wasNull() {
            return getDouble() == null;
        }

        public Object getObjectValue() {
            return getDouble();
        }
    }

    static class IntegerGlobAccessor implements IntegerAccessor {
        private final IntegerField field;
        private final Ref<Glob> glob;

        IntegerGlobAccessor(IntegerField field, Ref<Glob> glob) {
            this.field = field;
            this.glob = glob;
        }

        public Integer getInteger() {
            return glob.get().get(field);
        }

        public int getValue(int valueIfNull) {
            return glob.get().get(field, valueIfNull);
        }

        public boolean wasNull() {
            return getInteger() == null;
        }

        public Object getObjectValue() {
            return getInteger();
        }
    }
    static class LongGlobAccessor implements LongAccessor {
        private final LongField field;
        private final Ref<Glob> glob;

        LongGlobAccessor(LongField field, Ref<Glob> glob) {
            this.field = field;
            this.glob = glob;
        }

        public Long getLong() {
            return glob.get().get(field);
        }

        public long getValue(long valueIfNull) {
            return glob.get().get(field, valueIfNull);
        }

        public boolean wasNull() {
            return getLong() == null;
        }

        public Object getObjectValue() {
            return getLong();
        }
    }

    static class BooleanGlobAccessor implements BooleanAccessor {
        private final BooleanField field;
        private final Ref<Glob> glob;

        BooleanGlobAccessor(BooleanField field, Ref<Glob> glob) {
            this.field = field;
            this.glob = glob;
        }

        public Boolean getBoolean() {
            return glob.get().get(field);
        }

        public boolean getValue(boolean valueIfNull) {
            return glob.get().get(field, valueIfNull);
        }

        public Object getObjectValue() {
            return getBoolean();
        }
    }

    static class StringGlobAccessor implements StringAccessor {
        private final StringField field;
        private final Ref<Glob> glob;

        StringGlobAccessor(StringField field, Ref<Glob> glob) {
            this.field = field;
            this.glob = glob;
        }

        public String getString() {
            return glob.get().get(field);
        }

        public Object getObjectValue() {
            return getString();
        }
    }
}
