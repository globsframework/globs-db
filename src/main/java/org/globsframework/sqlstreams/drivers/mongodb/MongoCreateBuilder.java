package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.MongoDatabase;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.BulkDbRequest;
import org.globsframework.sqlstreams.CreateBuilder;
import org.globsframework.sqlstreams.SqlRequest;
import org.globsframework.streams.accessors.*;
import org.globsframework.streams.accessors.utils.*;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.HashMap;
import java.util.Map;

public class MongoCreateBuilder implements CreateBuilder {
    private final MongoDatabase mongoDatabase;
    private final GlobType globType;
    private final MongoDbService sqlService;
    Map<Field, Accessor> fieldsValues = new HashMap<>();

    public MongoCreateBuilder(MongoDatabase mongoDatabase, GlobType globType, MongoDbService sqlService) {
        this.mongoDatabase = mongoDatabase;
        this.globType = globType;
        this.sqlService = sqlService;
    }

    public CreateBuilder set(IntegerField field, Integer value) {
        fieldsValues.put(field, new ValueIntegerAccessor(value));
        return this;
    }

    public CreateBuilder set(BlobField field, byte[] value) {
        fieldsValues.put(field, new ValueBlobAccessor(value));
        return this;
    }

    public CreateBuilder set(StringField field, String value) {
        fieldsValues.put(field, new ValueStringAccessor(value));
        return this;
    }

    public CreateBuilder set(DoubleField field, Double value) {
        fieldsValues.put(field, new ValueDoubleAccessor(value));
        return this;
    }

    public CreateBuilder set(BooleanField field, Boolean value) {
        fieldsValues.put(field, new ValueBooleanAccessor(value));
        return this;
    }

    public CreateBuilder set(DateField field, LocalDate value) {
        fieldsValues.put(field, new ValueDateAccessor(value));
        return this;
    }

    public CreateBuilder set(DateTimeField field, ZonedDateTime value) {
        fieldsValues.put(field, new ValueDateTimeAccessor(value));
        return this;
    }

    public CreateBuilder set(LongField field, Long value) {
        fieldsValues.put(field, new ValueLongAccessor(value));
        return this;
    }

    public CreateBuilder set(IntegerField field, IntegerAccessor accessor) {
        fieldsValues.put(field, accessor);
        return this;
    }

    public CreateBuilder set(LongField field, LongAccessor accessor) {
        fieldsValues.put(field, accessor);
        return this;
    }

    public CreateBuilder set(StringField field, StringAccessor accessor) {
        fieldsValues.put(field, accessor);
        return this;
    }

    public CreateBuilder set(DoubleField field, DoubleAccessor accessor) {
        fieldsValues.put(field, accessor);
        return this;
    }

    public CreateBuilder set(BooleanField field, BooleanAccessor accessor) {
        fieldsValues.put(field, accessor);
        return this;
    }

    public CreateBuilder set(BlobField field, BlobAccessor accessor) {
        fieldsValues.put(field, accessor);
        return this;
    }

    public CreateBuilder set(GlobField field, Glob value) {
        fieldsValues.put(field, new ValueGlobAccessor(value));
        return this;
    }

    public CreateBuilder set(GlobArrayField field, Glob[] values) {
        fieldsValues.put(field, new ValueGlobsAccessor(values));
        return this;
    }

    public CreateBuilder set(GlobField field, GlobAccessor accessor) {
        fieldsValues.put(field, accessor);
        return this;
    }

    public CreateBuilder set(GlobArrayField field, GlobsAccessor accessor) {
        fieldsValues.put(field, accessor);
        return this;
    }
    public CreateBuilder set(GlobUnionField field, Glob value) {
        fieldsValues.put(field, new ValueGlobAccessor(value));
        return this;
    }

    public CreateBuilder set(GlobArrayUnionField field, Glob[] values) {
        fieldsValues.put(field, new ValueGlobsAccessor(values));
        return this;
    }

    public CreateBuilder set(GlobUnionField field, GlobAccessor accessor) {
        fieldsValues.put(field, accessor);
        return this;
    }

    public CreateBuilder set(GlobArrayUnionField field, GlobsAccessor accessor) {
        fieldsValues.put(field, accessor);
        return this;
    }

    public CreateBuilder set(DateTimeField field, DateTimeAccessor accessor) {
        fieldsValues.put(field, accessor);
        return this;
    }

    public CreateBuilder set(DateField field, DateAccessor accessor) {
        fieldsValues.put(field, accessor);
        return this;
    }

    public CreateBuilder setObject(Field field, Accessor accessor) {
        fieldsValues.put(field, accessor);
        return this;
    }

    public CreateBuilder setObject(Field field, Object value) {
        field.safeVisit(new FieldValueVisitor.AbstractWithErrorVisitor() {
            public void visitInteger(IntegerField field, Integer value) {
                set(field, value);
            }

            public void visitDouble(DoubleField field, Double value) {
                set(field, value);
            }

            public void visitString(StringField field, String value) {
                set(field, value);
            }

            public void visitBoolean(BooleanField field, Boolean value) {
                set(field, value);
            }

            public void visitLong(LongField field, Long value) {
                set(field, value);
            }

            public void visitBlob(BlobField field, byte[] value) {
                set(field, value);
            }
        }, value);
        return this;
    }

    public SqlRequest getRequest() {
        return new MongoCreateSqlRequest(mongoDatabase.getCollection(sqlService.getTableName(globType)), fieldsValues, sqlService, false);
    }

    public BulkDbRequest getBulkRequest() {
        return new MongoCreateSqlRequest(mongoDatabase.getCollection(sqlService.getTableName(globType)), fieldsValues, sqlService, true);
    }

}
