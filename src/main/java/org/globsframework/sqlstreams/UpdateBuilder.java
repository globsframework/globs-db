package org.globsframework.sqlstreams;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.accessors.LongSqlAccessor;
import org.globsframework.streams.accessors.*;

import java.time.LocalDate;
import java.time.ZonedDateTime;

public interface UpdateBuilder {

    UpdateBuilder updateUntyped(Field field, Object value);

    UpdateBuilder updateUntyped(Field field, Accessor accessor);

    UpdateBuilder update(IntegerField field, IntegerAccessor accessor);

    UpdateBuilder update(IntegerField field, Integer value);

    UpdateBuilder update(LongField field, LongAccessor accessor);

    UpdateBuilder update(LongField field, Long value);

    UpdateBuilder update(DoubleField field, DoubleAccessor accessor);

    UpdateBuilder update(DoubleField field, Double value);

    UpdateBuilder update(StringField field, StringAccessor accessor);

    UpdateBuilder update(StringArrayField field, StringArrayAccessor accessor);

    UpdateBuilder update(StringField field, String value);

    UpdateBuilder update(DateTimeField field, ZonedDateTime value);

    UpdateBuilder update(DateField field, LocalDate value);

    UpdateBuilder update(DateTimeField field, DateTimeAccessor accessor);

    UpdateBuilder update(DateField field, DateAccessor accessor);

    UpdateBuilder update(StringArrayField field, String[] value);

    UpdateBuilder update(BooleanField field, BooleanAccessor accessor);

    UpdateBuilder update(BooleanField field, Boolean value);

    UpdateBuilder update(BlobField field, byte[] value);

    UpdateBuilder update(BlobField field, BlobAccessor accessor);

    UpdateBuilder update(GlobField field, GlobAccessor accessor);

    UpdateBuilder update(GlobField field, Glob value);

    UpdateBuilder update(GlobArrayField field, GlobsAccessor accessor);

    UpdateBuilder update(GlobArrayField field, Glob[] values);

    UpdateBuilder update(LongArrayField field, LongArrayAccessor accessor);

    UpdateBuilder update(LongArrayField field, long[] values);

    SqlRequest getRequest();

    BulkDbRequest getBulkRequest();
}
