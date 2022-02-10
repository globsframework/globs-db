package org.globsframework.sqlstreams.drivers.jdbc.request;

import org.globsframework.json.GSonUtils;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.annotations.IsDate;
import org.globsframework.metamodel.annotations.IsDateTime;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.SelectBuilder;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.accessors.*;
import org.globsframework.sqlstreams.annotations.IsTimestamp;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.jdbc.*;
import org.globsframework.streams.accessors.*;
import org.globsframework.utils.Ref;

import java.io.StringReader;
import java.sql.Connection;
import java.util.*;

public class SqlQueryBuilder implements SelectBuilder {
    private final List<Order> orders = new ArrayList<>();
    private Connection connection;
    private GlobType globType;
    private Constraint constraint;
    private SqlService sqlService;
    private BlobUpdater blobUpdater;
    private boolean autoClose = true;
    private Map<Field, SqlAccessor> fieldToAccessorHolder = new HashMap<Field, SqlAccessor>();
    private int top = -1;
    private Set<Field> distinct = new HashSet<>();
    private List<SqlOperation> sqlOperations = new ArrayList<>();
    private List<Field> groupBy = new ArrayList<>();

    public SqlQueryBuilder(Connection connection, GlobType globType, Constraint constraint, SqlService sqlService, BlobUpdater blobUpdater) {
        this.connection = connection;
        this.globType = globType;
        this.constraint = constraint;
        this.sqlService = sqlService;
        this.blobUpdater = blobUpdater;
    }

    public SelectQuery getQuery() {
        try {
            return new SqlSelectQuery(connection, constraint, fieldToAccessorHolder, sqlService, blobUpdater, autoClose,
                    orders, groupBy, top, distinct, sqlOperations);
        } finally {
            fieldToAccessorHolder.clear();
        }
    }

    public SelectQuery getNotAutoCloseQuery() {
        autoClose = false;
        return getQuery();
    }

    public SelectBuilder withKeys() {
        completeWithKeys();
        return this;
    }

    public IntegerAccessor max(IntegerField field) {
        return singleOp(field, "MAX");
    }

    private IntegerAccessor singleOp(IntegerField field, String op) {
        IntegerAccessor accessor = createAccessor(field);
        sqlOperations.add(new SqlOperation() {

            public SqlAccessor getAccessor() {
                return (SqlAccessor) accessor;
            }

            public String toSqlOpe(ToSqlName toSqlName) {
                return op + "(" + toSqlName.toSqlName(field) + ")";
            }
        });
        return accessor;
    }

    public LongAccessor max(LongField field) {
        return singleOp(field, "MAX");
    }

    public LongAccessor count(LongField field) {
        return singleOp(field, "COUNT");
    }

    public LongAccessor count(IntegerField field) {
        return singleLongOp(field, "COUNT");
    }

    public LongAccessor sum(IntegerField field) {
        return singleLongOp(field, "SUM");
    }

    public LongAccessor sum(LongField field) {
        return singleOp(field, "SUM");
    }

    private LongAccessor singleOp(LongField field, String op) {
        LongAccessor accessor = createAccessor(field);
        sqlOperations.add(new SqlOperation() {

            public SqlAccessor getAccessor() {
                return (SqlAccessor) accessor;
            }

            public String toSqlOpe(ToSqlName toSqlName) {
                return op + "(" + toSqlName.toSqlName(field) + ")";
            }
        });
        return accessor;
    }

    private LongAccessor singleLongOp(IntegerField field, String op) {
        LongAccessor accessor = new LongSqlAccessor();
        sqlOperations.add(new SqlOperation() {

            public SqlAccessor getAccessor() {
                return (SqlAccessor) accessor;
            }

            public String toSqlOpe(ToSqlName toSqlName) {
                return op + "(" + toSqlName.toSqlName(field) + ")";
            }
        });
        return accessor;
    }

    public IntegerAccessor min(IntegerField field) {
        return singleOp(field, "MIN");
    }

    public LongAccessor min(LongField field) {
        return singleOp(field, "MIN");
    }

    private void completeWithKeys() {
        for (Field field : globType.getKeyFields()) {
            if (!fieldToAccessorHolder.containsKey(field)) {
                select(field);
            }
        }
    }

    public SelectBuilder select(Field field) {
        retrieveUnTyped(field);
        return this;
    }

    public SelectBuilder selectAll() {
        for (Field field : globType.getFields()) {
            select(field);
        }
        return this;
    }

    public SelectBuilder select(IntegerField field, Ref<IntegerAccessor> ref) {
        ref.set(retrieve(field));
        return this;
    }

    public SelectBuilder select(LongField field, Ref<LongAccessor> ref) {
        ref.set(retrieve(field));
        return this;
    }

    public SelectBuilder select(BooleanField field, Ref<BooleanAccessor> ref) {
        ref.set(retrieve(field));
        return this;
    }

    public SelectBuilder select(StringField field, Ref<StringAccessor> ref) {
        ref.set(retrieve(field));
        return this;
    }

    public SelectBuilder select(DoubleField field, Ref<DoubleAccessor> ref) {
        ref.set(retrieve(field));
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
        this.groupBy.add(field);
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

    public SelectBuilder distinct(Collection<Field> fields) {
        this.distinct.addAll(fields);
        return this;
    }

    public BooleanAccessor retrieve(BooleanField field) {
        return (BooleanAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> new BooleanSqlAccessor());
    }

    public IntegerAccessor retrieve(IntegerField field) {
        return (IntegerAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> (SqlAccessor) createAccessor(field));
    }

    private IntegerAccessor createAccessor(IntegerField field) {
        IntegerAccessor accessor;
        if (field.hasAnnotation(IsDate.KEY)) {
            accessor = new DateIntegerSqlAccessor();
        } else {
            accessor = new IntegerSqlAccessor();
        }
        return accessor;
    }

    private DateAccessor createAccessor(DateField field) {
        return new DateSqlAccessor();
    }

    private DateTimeAccessor createAccessor(DateTimeField field) {
        return new DateTimeSqlAccessor();
    }

    public LongAccessor retrieve(LongField field) {
        return (LongAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> (SqlAccessor) createAccessor(field));
    }

    public DateAccessor retrieve(DateField field) {
        return (DateAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> (SqlAccessor) createAccessor(field));
    }

    public DateTimeAccessor retrieve(DateTimeField field) {
        return (DateTimeAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> (SqlAccessor) createAccessor(field));
    }

    private LongAccessor createAccessor(LongField field) {
        LongAccessor accessor;
        if (field.hasAnnotation(IsDate.KEY)) {
            accessor = new DateLongSqlAccessor();
        } else if (field.hasAnnotation(IsDateTime.KEY)) {
            accessor = new DateTimeLongSqlAccessor();
        } else if (field.hasAnnotation(IsTimestamp.KEY)) {
            accessor = new DateTimeLongSqlAccessor();
        } else {
            accessor = new LongSqlAccessor();
        }
        return accessor;
    }

    public StringAccessor retrieve(StringField field) {
        return (StringAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> new StringSqlAccessor());
    }

    public DoubleAccessor retrieve(DoubleField field) {
        return (DoubleAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> new DoubleSqlAccessor());
    }

    public BlobSqlAccessor retrieve(BlobField field) {
        return (BlobSqlAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> new BlobSqlAccessor());
    }

    public Accessor retrieveUnTyped(Field field) {
        AccessorToFieldVisitor visitor = new AccessorToFieldVisitor();
        field.safeVisit(visitor);
        return visitor.get();
    }

    public StringArrayAccessor retrieve(StringArrayField field) {
        return (StringArrayAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> new StringArraySqlAccessor(new StringSqlAccessor()));
    }

    public GlobAccessor retrieve(GlobField field) {
        return (GlobAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> new GlobSqlAccessor(new StringSqlAccessor(), field.getTargetType()));
    }

    public GlobsAccessor retrieve(GlobArrayField field) {
        return (GlobsAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> new GlobsSqlAccessor(new StringSqlAccessor(), field.getTargetType()));
    }

    public static class Order {
        public final Field field;
        public final boolean asc;

        public Order(Field field, boolean asc) {
            this.field = field;
            this.asc = asc;
        }
    }

    private static class GlobSqlAccessor extends SqlAccessor implements GlobAccessor {
        private final StringSqlAccessor accessor;
        private GlobType type;

        public GlobSqlAccessor(StringSqlAccessor accessor, GlobType type) {
            this.accessor = accessor;
            this.type = type;
        }

        public void setMoStream(SqlDbStream sqlMoStream) {
            super.setMoStream(sqlMoStream);
            accessor.setMoStream(sqlMoStream);
        }

        public void setIndex(int index) {
            super.setIndex(index);
            accessor.setIndex(index);
        }

        public Glob getGlob() {
            String value = accessor.getString();
            if (value != null) {
                return GSonUtils.decode(new StringReader(value), type);
            } else {
                return null;
            }
        }

        public Object getObjectValue() {
            return getGlob();
        }
    }

    private static class StringArraySqlAccessor extends SqlAccessor implements StringArrayAccessor {
        private final StringSqlAccessor accessor;

        public StringArraySqlAccessor(StringSqlAccessor accessor) {
            this.accessor = accessor;
        }

        public void setMoStream(SqlDbStream sqlMoStream) {
            super.setMoStream(sqlMoStream);
            accessor.setMoStream(sqlMoStream);
        }

        public void setIndex(int index) {
            super.setIndex(index);
            accessor.setIndex(index);
        }

        public String[] getString() {
            String value = accessor.getString();
            if (value != null) {
                if (value.isEmpty()) {
                    return new String[0];
                } else {
                    return value.split(",");
                }
            } else {
                return null;
            }
        }

        public Object getObjectValue() {
            return getString();
        }
    }

    private static class GlobsSqlAccessor extends SqlAccessor implements GlobsAccessor {
        private final StringSqlAccessor accessor;
        private GlobType type;

        public GlobsSqlAccessor(StringSqlAccessor accessor, GlobType type) {
            this.accessor = accessor;
            this.type = type;
        }

        public void setMoStream(SqlDbStream sqlMoStream) {
            super.setMoStream(sqlMoStream);
            accessor.setMoStream(sqlMoStream);
        }

        public void setIndex(int index) {
            super.setIndex(index);
            accessor.setIndex(index);
        }

        public Glob[] getGlobs() {
            String value = accessor.getString();
            if (value != null) {
                return GSonUtils.decodeArray(new StringReader(value), type);
            } else {
                return null;
            }
        }

        public Object getObjectValue() {
            return getGlobs();
        }
    }

    private class AccessorToFieldVisitor extends FieldVisitor.AbstractWithErrorVisitor {
        private Accessor accessor;

        public AccessorToFieldVisitor() {
        }

        public void visitInteger(IntegerField field) {
            accessor = retrieve(field);
        }

        public void visitDouble(DoubleField field) {
            accessor = retrieve(field);
        }

        public void visitString(StringField field) {
            accessor = retrieve(field);
        }

        public void visitBoolean(BooleanField field) {
            accessor = retrieve(field);
        }

        public void visitBlob(BlobField field) {
            accessor = retrieve(field);
        }

        public void visitLong(LongField field) {
            accessor = retrieve(field);
        }

        public void visitDate(DateField field) throws Exception {
            accessor = retrieve(field);
        }

        public void visitDateTime(DateTimeField field) throws Exception {
            accessor = retrieve(field);
        }

        public void visitStringArray(StringArrayField field) throws Exception {
            accessor = retrieve(field);
        }

        public void visitGlob(GlobField field) throws Exception {
            accessor = retrieve(field);
        }

        public void visitGlobArray(GlobArrayField field) throws Exception {
            accessor = retrieve(field);
        }

        public Accessor get() {
            return accessor;
        }
    }
}

