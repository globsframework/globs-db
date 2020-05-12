package org.globsframework.sqlstreams.drivers.jdbc.request;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.sqlstreams.BulkDbRequest;
import org.globsframework.sqlstreams.SqlRequest;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.UpdateBuilder;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.jdbc.BlobUpdater;
import org.globsframework.sqlstreams.drivers.jdbc.SqlUpdateRequest;
import org.globsframework.sqlstreams.exceptions.SqlException;
import org.globsframework.streams.accessors.*;
import org.globsframework.streams.accessors.utils.*;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

public class SqlUpdateBuilder implements UpdateBuilder {
    private Map<Field, Accessor> values = new HashMap<Field, Accessor>();
    private Connection connection;
    private GlobType globType;
    private SqlService sqlService;
    private Constraint constraint;
    private BlobUpdater blobUpdater;

    public SqlUpdateBuilder(Connection connection, GlobType globType, SqlService sqlService,
                            Constraint constraint, BlobUpdater blobUpdater) {
        this.blobUpdater = blobUpdater;
        this.connection = connection;
        this.globType = globType;
        this.sqlService = sqlService;
        this.constraint = constraint;
    }

    public UpdateBuilder updateUntyped(Field field, final Object value) {
        field.safeVisit(new FieldVisitor.AbstractWithErrorVisitor() {
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

            public void visitBlob(BlobField field) {
                update(field, (byte[]) value);
            }

        });
        return this;
    }

    public UpdateBuilder updateUntyped(Field field, Accessor accessor) {
        values.put(field, accessor);
        return this;
    }

    public UpdateBuilder update(IntegerField field, IntegerAccessor accessor) {
        values.put(field, accessor);
        return this;
    }

    public UpdateBuilder update(IntegerField field, Integer value) {
        return update(field, new ValueIntegerAccessor(value));
    }

    public UpdateBuilder update(LongField field, LongAccessor accessor) {
        values.put(field, accessor);
        return this;
    }

    public UpdateBuilder update(LongField field, Long value) {
        return update(field, new ValueLongAccessor(value));
    }

    public UpdateBuilder update(DoubleField field, DoubleAccessor accessor) {
        values.put(field, accessor);
        return this;
    }

    public UpdateBuilder update(DoubleField field, Double value) {
        return update(field, new ValueDoubleAccessor(value));
    }

    public UpdateBuilder update(StringField field, StringAccessor accessor) {
        values.put(field, accessor);
        return this;
    }

    public UpdateBuilder update(StringArrayField field, StringArrayAccessor accessor) {
        values.put(field, accessor);
        return this;
    }

    public UpdateBuilder update(StringField field, String value) {
        return update(field, new ValueStringAccessor(value));
    }

    public UpdateBuilder update(StringArrayField field, String[] value) {
        return update(field, new ValueStringArrayAccessor(value));
    }

    public UpdateBuilder update(BooleanField field, BooleanAccessor accessor) {
        values.put(field, accessor);
        return this;
    }

    public UpdateBuilder update(BooleanField field, Boolean value) {
        return update(field, new ValueBooleanAccessor(value));
    }

    public UpdateBuilder update(BlobField field, byte[] value) {
        return update(field, new ValueBlobAccessor(value));
    }

    public UpdateBuilder update(BlobField field, BlobAccessor accessor) {
        values.put(field, accessor);
        return this;
    }

    public UpdateBuilder update(GlobField field, GlobAccessor accessor) {
        values.put(field, accessor);
        return this;
    }

    public UpdateBuilder update(GlobArrayField field, GlobsAccessor accessor) {
        values.put(field, accessor);
        return this;
    }

    public SqlRequest getRequest() {
        try {
            return new SqlUpdateRequest(globType, constraint, values, connection, sqlService, blobUpdater);
        } finally {
            values.clear();
        }
    }

    public BulkDbRequest getBulkRequest() {
        SqlRequest request = getRequest();
        return new BulkDbRequest() {
            public void flush() {
            }

            public void run() throws SqlException {
                request.run();
            }

            public void close() {
                request.close();
            }
        };
    }
}
