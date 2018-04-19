package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import org.bson.Document;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.metamodel.index.Index;
import org.globsframework.metamodel.index.impl.IsUniqueIndexVisitor;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.BulkDbRequest;
import org.globsframework.sqlstreams.CreateBuilder;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.annotations.DbFieldName;
import org.globsframework.sqlstreams.annotations.IsDbKey;
import org.globsframework.streams.accessors.*;
import org.globsframework.utils.Ref;
import org.globsframework.utils.collections.MultiMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class MongoUtils {
    public static final String DB_REF_ID_EXT = "id";
    public static final String DB_REF_REF_EXT = "ref";
    private static Logger LOGGER = LoggerFactory.getLogger(MongoUtils.class);

    public static void createIndexIfNeeded(MongoCollection<?> collection, Collection<Index> indices, SqlService sqlService) {
        if (indices.isEmpty()) {
            return;
        }
        List<Document> documents = new ArrayList<>();
        collection.listIndexes().into(documents);
        for (Index index : indices) {
            findOrCreateIndex(collection, index, documents, sqlService);
        }
    }

    private static void findOrCreateIndex(MongoCollection<?> collection, Index functionalIndex, List<Document> documents, SqlService sqlService) {
        for (Document document : documents) {
            if (contain(functionalIndex, document, sqlService)) {
                return;
            }
        }
        Document document = new Document();
        functionalIndex.fields().forEach(field -> document.append(sqlService.getColumnName(field), 1));
        LOGGER.info("create index " + functionalIndex.getName() + " =>" + document);
        collection.createIndex(document, new IndexOptions()
                .unique(functionalIndex.visit(new IsUniqueIndexVisitor()).isUnique())
                .name(functionalIndex.getName()));
    }

    protected static boolean contain(Index functionalIndex, Document document, SqlService sqlService) {
        Document key = document.get("key", Document.class);
        return functionalIndex.fields().count() == key.entrySet().size() &&
                functionalIndex.fields().allMatch(field -> key.containsKey(sqlService.getColumnName(field)));
    }

    public static String getDbName(Field field) {
        Glob name = field.findAnnotation(DbFieldName.KEY);
        if (name != null) {
            return name.get(DbFieldName.NAME);
        }
        if (field.isKeyField() && field.hasAnnotation(IsDbKey.KEY)) {
            return MongoDbService.ID_FIELD_NAME;
        }
        return field.getName();
    }


    public static void fill(List<Glob> data, SqlService sqlService) {
        MultiMap<GlobType, Glob> dataByType = new MultiMap<>();
        data.forEach(glob -> dataByType.put(glob.getType(), glob));
        for (Map.Entry<GlobType, List<Glob>> globTypeListEntry : dataByType.entries()) {
            Ref<Glob> ref = new Ref<>();
            GlobType globType = globTypeListEntry.getKey();
            CreateBuilder createBuilder = sqlService.getDb().getCreateBuilder(globType);
            for (Field field : globType.getFields()) {
                Accessor accessor = field.safeVisit(new FieldVisitor.AbstractWithErrorVisitor() {
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

                    public void visitGlob(GlobField field) throws Exception {
                        accessor = new MongoGlobAccessor(field, ref);
                    }

                    public void visitGlobArray(GlobArrayField field) throws Exception {
                        accessor = new MongoGlobArrayAccessor(field, ref);
                    }
                }).accessor;
                createBuilder.setObject(field, accessor);
            }
            BulkDbRequest request = createBuilder.getBulkRequest();
            for (Glob glob : globTypeListEntry.getValue()) {
                ref.set(glob);
                request.run();
            }
            request.close();
        }
    }

    static class DoubleGlobAccessor implements DoubleAccessor {
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

    static class MongoGlobArrayAccessor implements GlobsAccessor {
        private final GlobArrayField field;
        private final Ref<Glob> glob;

        MongoGlobArrayAccessor(GlobArrayField field, Ref<Glob> glob) {
            this.field = field;
            this.glob = glob;
        }

        public Glob[] getGlobs() {
            return glob.get().get(field);
        }

        public Object getObjectValue() {
            return getGlobs();
        }
    }

    static class MongoGlobAccessor implements GlobAccessor {
        private final GlobField field;
        private final Ref<Glob> glob;

        MongoGlobAccessor(GlobField field, Ref<Glob> glob) {
            this.field = field;
            this.glob = glob;
        }

        public Glob getGlob() {
            return glob.get().get(field);
        }

        public Object getObjectValue() {
            return getGlob();
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
