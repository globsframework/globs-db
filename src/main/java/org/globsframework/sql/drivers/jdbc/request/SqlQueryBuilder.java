package org.globsframework.sql.drivers.jdbc.request;

import com.google.gson.Gson;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeResolver;
import org.globsframework.core.metamodel.annotations.IsDate;
import org.globsframework.core.metamodel.annotations.IsDateTime;
import org.globsframework.core.metamodel.fields.*;
import org.globsframework.core.model.Glob;
import org.globsframework.core.streams.accessors.*;
import org.globsframework.core.utils.Ref;
import org.globsframework.json.GSonUtils;
import org.globsframework.sql.SelectBuilder;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.accessors.*;
import org.globsframework.sql.annotations.IsTimestamp;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.*;

import java.io.IOException;
import java.io.StringReader;
import java.sql.Connection;
import java.util.*;

public class SqlQueryBuilder implements SelectBuilder {
    protected final List<Order> orders = new ArrayList<>();
    protected Connection connection;
    private GlobType globType;
    protected Constraint constraint;
    protected SqlService sqlService;
    protected BlobUpdater blobUpdater;
    protected boolean autoClose = true;
    protected Map<Field, SqlAccessor> fieldToAccessorHolder = new HashMap<Field, SqlAccessor>();
    protected int top = -1;
    protected int skip = -1;
    protected Set<Field> distinct = new HashSet<>();
    protected List<SqlOperation> sqlOperations = new ArrayList<>();
    protected List<Field> groupBy = new ArrayList<>();
    protected GlobType fallBackType = null;

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
                    orders, groupBy, top, skip, distinct, sqlOperations, fallBackType == null ? globType : fallBackType);
        } finally {
            fieldToAccessorHolder.clear();
        }
    }

    public SelectQuery getQuery(String sql) {
        try {
            return new SqlSelectQuery(sqlService, connection, sql, fieldToAccessorHolder, fallBackType == null ? globType : fallBackType);
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
        if (fallBackType == null) {
            fallBackType = field.getGlobType();
        }
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

    public LongAccessor count(Field field) {
        return singleCount(field, "COUNT");
    }

    public LongAccessor sum(IntegerField field) {
        return singleLongOp(field, "SUM");
    }

    public LongAccessor sum(LongField field) {
        return singleOp(field, "SUM");
    }

    private LongAccessor singleOp(LongField field, String op) {
        if (fallBackType == null) {
            fallBackType = field.getGlobType();
        }
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
        if (fallBackType == null) {
            fallBackType = field.getGlobType();
        }
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

    private LongAccessor singleCount(Field field, String op) {
        if (fallBackType == null) {
            fallBackType = field.getGlobType();
        }
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

    public SelectBuilder select(BytesField field, Ref<BytesAccessor> accessor) {
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

    public SelectBuilder select(LongArrayField field, Ref<LongArrayAccessor> accessor) {
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

    public SelectBuilder skip(int n) {
        skip = n;
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

    public BytesSqlAccessor retrieve(BytesField field) {
        return (BytesSqlAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> new BytesSqlAccessor());
    }

    public Accessor retrieveUnTyped(Field field) {
        AccessorToFieldVisitor visitor = new AccessorToFieldVisitor();
        field.safeAccept(visitor);
        return visitor.get();
    }

    public StringArrayAccessor retrieve(StringArrayField field) {
        return (StringArrayAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> new StringArraySqlAccessor(new StringSqlAccessor()));
    }

    public GlobAccessor retrieve(GlobField field) {
        return (GlobAccessor) fieldToAccessorHolder.computeIfAbsent(field,
                x -> new GlobSqlAccessor(new StringSqlAccessor(), GlobTypeResolver.from(field.getTargetType())));
    }

    public GlobAccessor retrieve(GlobUnionField field) {
        return (GlobAccessor) fieldToAccessorHolder.computeIfAbsent(field,
                x -> new GlobSqlAccessor(new StringSqlAccessor(), GlobTypeResolver.from(field.getTargetTypes())));
    }

    public GlobsAccessor retrieve(GlobArrayField field) {
        return (GlobsAccessor) fieldToAccessorHolder.computeIfAbsent(field,
                x -> new GlobsSqlAccessor(new StringSqlAccessor(), GlobTypeResolver.from(field.getTargetType())));
    }

    public GlobsAccessor retrieve(GlobArrayUnionField field) {
        return (GlobsAccessor) fieldToAccessorHolder.computeIfAbsent(field,
                x -> new GlobsSqlAccessor(new StringSqlAccessor(), GlobTypeResolver.from(field.getTargetTypes())));
    }

    public IntegerArrayAccessor retrieve(IntegerArrayField field) {
        return (IntegerArrayAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> new IntegerArraySqlAccessor(new StringSqlAccessor()));
    }

    public LongArrayAccessor retrieve(LongArrayField field) {
        return (LongArrayAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> new LongArraySqlAccessor(new StringSqlAccessor()));
    }

    public DoubleArrayAccessor retrieve(DoubleArrayField field) {
        return (DoubleArrayAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> new DoubleArraySqlAccessor(new StringSqlAccessor()));
    }

    public BooleanArrayAccessor retrieve(BooleanArrayField field) {
        return (BooleanArrayAccessor) fieldToAccessorHolder.computeIfAbsent(field, x -> new BooleanArraySqlAccessor(new StringSqlAccessor()));
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
        private final GlobTypeResolver typeResolver;

        public GlobSqlAccessor(StringSqlAccessor accessor, GlobTypeResolver typeResolver) {
            this.accessor = accessor;
            this.typeResolver = typeResolver;
        }

        public void setMoStream(SqlGlobStream sqlMoStream) {
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
                return GSonUtils.decode(new StringReader(value), typeResolver);
            } else {
                return null;
            }
        }

        public Object getObjectValue() {
            return getGlob();
        }
    }

    private static class StringArraySqlAccessor extends SqlAccessor implements StringArrayAccessor {
        private static final Gson gson = new Gson();
        private static final TypeAdapter<?> adapter = gson.getAdapter(TypeToken.getArray(String.class));
        private final StringSqlAccessor accessor;

        public StringArraySqlAccessor(StringSqlAccessor accessor) {
            this.accessor = accessor;
        }

        public void setMoStream(SqlGlobStream sqlMoStream) {
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
                    if (value.charAt(0) != '[') {
                        return value.split(",");
                    } else {
                        try {
                            return (String[]) adapter.fromJson(value);
                        } catch (IOException e) {
                            throw new RuntimeException("For " + value, e);
                        }
                    }
                }
            } else {
                return null;
            }
        }

        public Object getObjectValue() {
            return getString();
        }
    }

    private abstract static class AbstractArraySqlAccessor extends SqlAccessor {
        protected final StringSqlAccessor accessor;

        public AbstractArraySqlAccessor(StringSqlAccessor accessor) {
            this.accessor = accessor;
        }

        public void setMoStream(SqlGlobStream sqlMoStream) {
            super.setMoStream(sqlMoStream);
            accessor.setMoStream(sqlMoStream);
        }

        public void setIndex(int index) {
            super.setIndex(index);
            accessor.setIndex(index);
        }
    }

    private static class IntegerArraySqlAccessor extends AbstractArraySqlAccessor implements IntegerArrayAccessor {
        public IntegerArraySqlAccessor(StringSqlAccessor accessor) {
            super(accessor);
        }

        public Object getObjectValue() {
            return getValues();
        }

        public int[] getValues() {
            String value = accessor.getString();
            if (value != null) {
                if (value.isEmpty()) {
                    return new int[0];
                } else {
                    if (value.charAt(0) == '[') {
                        value = value.substring(1, value.length() - 1);
                    }
                    return Arrays.stream(value.split(",")).mapToInt(Integer::parseInt).toArray();
                }
            } else {
                return null;
            }
        }
    }

    private static class LongArraySqlAccessor extends AbstractArraySqlAccessor implements LongArrayAccessor {
        public LongArraySqlAccessor(StringSqlAccessor accessor) {
            super(accessor);
        }

        public Object getObjectValue() {
            return getValues();
        }

        public long[] getValues() {
            String value = accessor.getString();
            if (value != null) {
                if (value.isEmpty()) {
                    return new long[0];
                } else {
                    if (value.charAt(0) == '[') {
                        value = value.substring(1, value.length() - 1);
                    }
                    return Arrays.stream(value.split(",")).mapToLong(Long::parseLong).toArray();
                }
            } else {
                return null;
            }
        }
    }

    private static class DoubleArraySqlAccessor extends AbstractArraySqlAccessor implements DoubleArrayAccessor {
        public DoubleArraySqlAccessor(StringSqlAccessor accessor) {
            super(accessor);
        }

        public Object getObjectValue() {
            return getValues();
        }

        public double[] getValues() {
            String value = accessor.getString();
            if (value != null) {
                if (value.isEmpty()) {
                    return new double[0];
                } else {
                    if (value.charAt(0) == '[') {
                        value = value.substring(1, value.length() - 1);
                    }
                    return Arrays.stream(value.split(",")).mapToDouble(Double::parseDouble).toArray();
                }
            } else {
                return null;
            }
        }
    }

    private static class BooleanArraySqlAccessor extends AbstractArraySqlAccessor implements BooleanArrayAccessor {
        public BooleanArraySqlAccessor(StringSqlAccessor accessor) {
            super(accessor);
        }

        public Object getObjectValue() {
            return getValues();
        }

        public boolean[] getValues() {
            String value = accessor.getString();
            if (value != null) {
                if (value.isEmpty()) {
                    return new boolean[0];
                } else {
                    if (value.charAt(0) == '[') {
                        value = value.substring(1, value.length() - 1);
                    }
                    String[] split = value.split(",");
                    boolean[] result = new boolean[split.length];
                    for (int i = 0; i < split.length; i++) {
                        result[i] = Boolean.parseBoolean(split[i]);
                    }
                    return result;
                }
            } else {
                return null;
            }
        }
    }

    private static class GlobsSqlAccessor extends SqlAccessor implements GlobsAccessor {
        private final StringSqlAccessor accessor;
        private GlobTypeResolver typeResolver;

        public GlobsSqlAccessor(StringSqlAccessor accessor, GlobTypeResolver typeResolver) {
            this.accessor = accessor;
            this.typeResolver = typeResolver;
        }

        public void setMoStream(SqlGlobStream sqlMoStream) {
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
                return GSonUtils.decodeArray(new StringReader(value), typeResolver);
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

        public void visitBytes(BytesField field) {
            accessor = retrieve(field);
        }

        public void visitLong(LongField field) {
            accessor = retrieve(field);
        }

        public void visitIntegerArray(IntegerArrayField field) throws Exception {
            accessor = retrieve(field);
        }

        public void visitLongArray(LongArrayField field) throws Exception {
            accessor = retrieve(field);
        }

        public void visitDoubleArray(DoubleArrayField field) throws Exception {
            accessor = retrieve(field);
        }

        public void visitBooleanArray(BooleanArrayField field) throws Exception {
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

        public void visitUnionGlob(GlobUnionField field) throws Exception {
            accessor = retrieve(field);
        }

        public void visitGlobArray(GlobArrayField field) throws Exception {
            accessor = retrieve(field);
        }

        public void visitUnionGlobArray(GlobArrayUnionField field) throws Exception {
            accessor = retrieve(field);
        }

        public Accessor get() {
            return accessor;
        }
    }
}

