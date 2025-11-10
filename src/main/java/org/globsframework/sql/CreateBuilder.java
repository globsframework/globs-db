package org.globsframework.sql;

import org.globsframework.core.metamodel.fields.*;
import org.globsframework.core.model.Glob;
import org.globsframework.core.streams.accessors.*;

import java.time.LocalDate;
import java.time.ZonedDateTime;

public interface CreateBuilder {
    CreateBuilder set(IntegerField field, Integer value);

    CreateBuilder set(BytesField field, byte[] value);

    CreateBuilder set(StringField field, String value);

    CreateBuilder set(StringArrayField field, String[] value);

    CreateBuilder set(LongField field, Long value);

    CreateBuilder set(LongArrayField field, long[] value);

    CreateBuilder set(DoubleField field, Double value);

    CreateBuilder set(BooleanField field, Boolean value);

    CreateBuilder set(DateTimeField field, ZonedDateTime value);

    CreateBuilder set(DateField field, LocalDate value);

    CreateBuilder set(GlobField field, Glob value);

    CreateBuilder set(GlobArrayField field, Glob[] values);

    CreateBuilder set(GlobUnionField field, Glob value);

    CreateBuilder set(GlobArrayUnionField field, Glob[] values);

    CreateBuilder set(IntegerField field, IntegerAccessor accessor);

    CreateBuilder set(LongField field, LongAccessor accessor);

    CreateBuilder set(StringField field, StringAccessor accessor);

    CreateBuilder set(StringArrayField field, StringArrayAccessor accessor);

    CreateBuilder set(DoubleField field, DoubleAccessor accessor);

    CreateBuilder set(BooleanField field, BooleanAccessor accessor);

    CreateBuilder set(BytesField field, BytesAccessor accessor);

    CreateBuilder set(GlobField field, GlobAccessor accessor);

    CreateBuilder set(GlobArrayField field, GlobsAccessor accessor);

    CreateBuilder set(GlobUnionField field, GlobAccessor accessor);

    CreateBuilder set(GlobArrayUnionField field, GlobsAccessor accessor);

    CreateBuilder set(DateTimeField field, DateTimeAccessor accessor);

    CreateBuilder set(DateField field, DateAccessor accessor);

    CreateBuilder setObject(Field field, Accessor accessor);

    CreateBuilder setObject(Field field, Object value);

    Accessor getKeyGeneratedAccessor(Field field);

    default LongAccessor getKeyGeneratedAccessor(LongField field) {
        return (LongAccessor) getKeyGeneratedAccessor((Field) field);
    }

    default StringAccessor getKeyGeneratedAccessor(StringField field) {
        return (StringAccessor) getKeyGeneratedAccessor((Field) field);
    }

    default IntegerAccessor getKeyGeneratedAccessor(IntegerField field) {
        return (IntegerAccessor) getKeyGeneratedAccessor((Field) field);

    }

    SqlRequest getRequest();

    BatchSqlRequest getBulkRequest();

}
