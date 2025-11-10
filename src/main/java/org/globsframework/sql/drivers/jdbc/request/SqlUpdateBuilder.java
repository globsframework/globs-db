package org.globsframework.sql.drivers.jdbc.request;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.*;
import org.globsframework.core.model.Glob;
import org.globsframework.core.streams.accessors.*;
import org.globsframework.core.streams.accessors.utils.*;
import org.globsframework.sql.BatchSqlRequest;
import org.globsframework.sql.SqlRequest;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.UpdateBuilder;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.BlobUpdater;
import org.globsframework.sql.drivers.jdbc.SqlUpdateRequest;

import java.sql.Connection;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

public class SqlUpdateBuilder implements UpdateBuilder {
    private final Map<Field, FieldWithAccessor> values = new HashMap<>();
    private final Connection connection;
    private final GlobType globType;
    private final SqlService sqlService;
    private final Constraint constraint;
    private final BlobUpdater blobUpdater;

    public SqlUpdateBuilder(Connection connection, GlobType globType, SqlService sqlService,
                            Constraint constraint, BlobUpdater blobUpdater) {
        this.blobUpdater = blobUpdater;
        this.connection = connection;
        this.globType = globType;
        this.sqlService = sqlService;
        this.constraint = constraint;
    }

    public record FieldWithAccessor(Field field, Accessor accessor) {}

    public UpdateBuilder updateUntyped(Field field, final Object value) {
        field.safeAccept(new FieldVisitor.AbstractWithErrorVisitor() {
            public void visitInteger(IntegerField field) {
                update(field, (Integer) value);
            }

            public void visitLong(LongField field) {
                update(field, (Long) value);
            }

            public void visitDouble(DoubleField field) {
                update(field, (Double) value);
            }

            public void visitString(StringField field) {
                update(field, (String) value);
            }

            public void visitBoolean(BooleanField field) {
                update(field, (Boolean) value);
            }

            public void visitBytes(BytesField field) {
                update(field, (byte[]) value);
            }

            public void visitGlob(GlobField field) {
                update(field, (Glob) value);
            }

            public void visitGlobArray(GlobArrayField field) {
                update(field, (Glob[]) value);
            }

            public void visitDate(DateField field) throws Exception {
                update(field, (LocalDate) value);
            }

            public void visitDateTime(DateTimeField field) throws Exception {
                update(field, (ZonedDateTime) value);
            }
        });
        return this;
    }

    public UpdateBuilder updateUntyped(Field field, Accessor accessor) {
        addToMap(field, accessor);
        return this;
    }

    public UpdateBuilder update(IntegerField field, IntegerAccessor accessor) {
        addToMap(field, accessor);
        return this;
    }

    public UpdateBuilder update(IntegerField field, Integer value) {
        return update(field, new ValueIntegerAccessor(value));
    }

    public UpdateBuilder update(LongField field, LongAccessor accessor) {
        addToMap(field, accessor);
        return this;
    }

    public UpdateBuilder update(LongField field, Long value) {
        return update(field, new ValueLongAccessor(value));
    }

    public UpdateBuilder update(DoubleField field, DoubleAccessor accessor) {
        addToMap(field, accessor);
        return this;
    }

    public UpdateBuilder update(DoubleField field, Double value) {
        return update(field, new ValueDoubleAccessor(value));
    }

    public UpdateBuilder update(StringField field, StringAccessor accessor) {
        addToMap(field, accessor);
        return this;
    }

    public UpdateBuilder update(StringArrayField field, StringArrayAccessor accessor) {
        addToMap(field, accessor);
        return this;
    }

    public UpdateBuilder update(StringField field, String value) {
        return update(field, new ValueStringAccessor(value));
    }

    public UpdateBuilder update(DateTimeField field, ZonedDateTime value) {
        return update(field, new ValueDateTimeAccessor(value));
    }

    public UpdateBuilder update(DateField field, LocalDate value) {
        return update(field, new ValueDateAccessor(value));
    }

    public UpdateBuilder update(DateTimeField field, DateTimeAccessor accessor) {
        addToMap(field, accessor);
        return this;
    }

    private void addToMap(Field f, Accessor accessor) {
        values.put(f, new FieldWithAccessor(f, accessor));
    }

    public UpdateBuilder update(DateField field, DateAccessor accessor) {
        addToMap(field, accessor);
        return this;
    }

    public UpdateBuilder update(StringArrayField field, String[] value) {
        return update(field, new ValueStringArrayAccessor(value));
    }

    public UpdateBuilder update(BooleanField field, BooleanAccessor accessor) {
        addToMap(field, accessor);
        return this;
    }

    public UpdateBuilder update(BooleanField field, Boolean value) {
        return update(field, new ValueBooleanAccessor(value));
    }

    public UpdateBuilder update(BytesField field, byte[] value) {
        return update(field, new ValueBytesAccessor(value));
    }

    public UpdateBuilder update(BytesField field, BytesAccessor accessor) {
        addToMap(field, accessor);
        return this;
    }

    public UpdateBuilder update(GlobField field, GlobAccessor accessor) {
        addToMap(field, accessor);
        return this;
    }

    public UpdateBuilder update(GlobArrayField field, GlobsAccessor accessor) {
        addToMap(field, accessor);
        return this;
    }

    public UpdateBuilder update(GlobField field, Glob value) {
        addToMap(field, new ValueGlobAccessor(value));
        return this;
    }

    public UpdateBuilder update(GlobArrayField field, Glob[] values) {
        addToMap(field, new ValueGlobsAccessor(values));
        return this;
    }

    public UpdateBuilder update(LongArrayField field, LongArrayAccessor accessor) {
        addToMap(field, accessor);
        return this;
    }

    public UpdateBuilder update(LongArrayField field, long[] values) {
        addToMap(field, new ValueLongArrayAccessor(values));
        return this;
    }

    public SqlRequest getRequest() {
        try {
            return new SqlUpdateRequest(globType, constraint, values.values().toArray(FieldWithAccessor[]::new), connection, sqlService, blobUpdater);
        } finally {
            values.clear();
        }
    }

    public BatchSqlRequest getBulkRequest() {
        try {
            return new SqlUpdateRequest(globType, constraint, values.values().toArray(FieldWithAccessor[]::new), connection, sqlService, blobUpdater);
        } finally {
            values.clear();
        }
    }
}
