package org.globsframework.sqlstreams.drivers.jdbc.request;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.sqlstreams.SelectBuilder;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.accessors.*;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.jdbc.BlobUpdater;
import org.globsframework.sqlstreams.drivers.jdbc.SqlSelectQuery;
import org.globsframework.sqlstreams.drivers.jdbc.impl.FieldToSqlAccessorVisitor;
import org.globsframework.sqlstreams.drivers.mongodb.MongoSelectBuilder;
import org.globsframework.streams.accessors.*;
import org.globsframework.utils.Ref;

import java.sql.Connection;
import java.util.*;

public class SqlQueryBuilder implements SelectBuilder {
    private Connection connection;
    private GlobType globType;
    private Constraint constraint;
    private SqlService sqlService;
    private BlobUpdater blobUpdater;
    private boolean autoClose = true;
    private Map<Field, SqlAccessor> fieldToAccessorHolder = new HashMap<Field, SqlAccessor>();
    private final List<Order> orders = new ArrayList<>();
    private int top = -1;
    private Set<Field> distinct = new HashSet<>();

    public static class Order {
        public final Field field;
        public final boolean asc;

        public Order(Field field, boolean asc) {
            this.field = field;
            this.asc = asc;
        }
    }

    public SqlQueryBuilder(Connection connection, GlobType globType, Constraint constraint, SqlService sqlService, BlobUpdater blobUpdater) {
        this.connection = connection;
        this.globType = globType;
        this.constraint = constraint;
        this.sqlService = sqlService;
        this.blobUpdater = blobUpdater;
    }

    public SelectQuery getQuery() {
        try {
            return new SqlSelectQuery(connection, constraint, fieldToAccessorHolder, sqlService, blobUpdater, autoClose, orders, top, distinct);
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

    private void completeWithKeys() {
        for (Field field : globType.getKeyFields()) {
            if (!fieldToAccessorHolder.containsKey(field)) {
                select(field);
            }
        }
    }

    public SelectBuilder select(Field field) {
        FieldToSqlAccessorVisitor visitor = new FieldToSqlAccessorVisitor();
        field.safeVisit(visitor);
        fieldToAccessorHolder.put(field, visitor.getAccessor());
        return this;
    }

    public SelectBuilder selectAll() {
        for (Field field : globType.getFields()) {
            select(field);
        }
        return this;
    }

    public SelectBuilder select(IntegerField field, Ref<IntegerAccessor> ref) {
        return createAccessor(field, ref, new IntegerSqlAccessor());
    }

    public SelectBuilder select(LongField field, Ref<LongAccessor> accessor) {
        return createAccessor(field, accessor, new LongSqlAccessor());
    }

    public SelectBuilder select(BooleanField field, Ref<BooleanAccessor> ref) {
        return createAccessor(field, ref, new BooleanSqlAccessor());
    }

    public SelectBuilder select(StringField field, Ref<StringAccessor> ref) {
        return createAccessor(field, ref, new StringSqlAccessor());
    }

    public SelectBuilder select(DoubleField field, Ref<DoubleAccessor> ref) {
        return createAccessor(field, ref, new DoubleSqlAccessor());
    }

    public SelectBuilder select(BlobField field, Ref<BlobAccessor> accessor) {
        return createAccessor(field, accessor, new BlobSqlAccessor());
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
        BooleanSqlAccessor accessor = new BooleanSqlAccessor();
        fieldToAccessorHolder.put(field, accessor);
        return accessor;
    }

    public IntegerAccessor retrieve(IntegerField field) {
        IntegerSqlAccessor accessor = new IntegerSqlAccessor();
        fieldToAccessorHolder.put(field, accessor);
        return accessor;
    }

    public LongAccessor retrieve(LongField field) {
        LongSqlAccessor accessor = new LongSqlAccessor();
        fieldToAccessorHolder.put(field, accessor);
        return accessor;
    }

    public StringAccessor retrieve(StringField field) {
        StringSqlAccessor accessor = new StringSqlAccessor();
        fieldToAccessorHolder.put(field, accessor);
        return accessor;
    }

    public DoubleAccessor retrieve(DoubleField field) {
        DoubleSqlAccessor accessor = new DoubleSqlAccessor();
        fieldToAccessorHolder.put(field, accessor);
        return accessor;
    }

    public BlobSqlAccessor retrieve(BlobField field) {
        BlobSqlAccessor accessor = new BlobSqlAccessor();
        fieldToAccessorHolder.put(field, accessor);
        return accessor;
    }

    public Accessor retrieveUnTyped(Field field) {
        AccessorToFieldVisitor visitor = new AccessorToFieldVisitor();
        field.safeVisit(visitor);
        return visitor.get();
    }

    public GlobAccessor retrieve(GlobField field) {
        throw new RuntimeException("Not Implemented " + field.getFullName());
    }

    private <T extends Accessor, D extends T> SelectBuilder createAccessor(Field field, Ref<T> ref, D accessor) {
        ref.set(accessor);
        fieldToAccessorHolder.put(field, (SqlAccessor) accessor);
        return this;
    }

    private class AccessorToFieldVisitor extends FieldVisitor.AbstractWithErrorVisitor {
        private Accessor accessor;

        public AccessorToFieldVisitor() {
        }

        public void visitInteger(IntegerField field) throws Exception {
            accessor = retrieve(field);
        }

        public void visitDouble(DoubleField field) throws Exception {
            accessor = retrieve(field);
        }

        public void visitString(StringField field) throws Exception {
            accessor = retrieve(field);
        }

        public void visitBoolean(BooleanField field) throws Exception {
            accessor = retrieve(field);
        }

        public void visitBlob(BlobField field) throws Exception {
            accessor = retrieve(field);
        }

        public void visitLong(LongField field) throws Exception {
            accessor = retrieve(field);
        }

        public Accessor get() {
            return accessor;
        }
    }
}

