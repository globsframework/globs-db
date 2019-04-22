package org.globsframework.sqlstreams;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.Glob;
import org.globsframework.streams.accessors.*;

public interface CreateBuilder {
    CreateBuilder set(IntegerField field, Integer value);

    CreateBuilder set(BlobField field, byte[] value);

    CreateBuilder set(StringField field, String value);

    CreateBuilder set(LongField field, Long value);

    CreateBuilder set(DoubleField field, Double value);

    CreateBuilder set(BooleanField field, Boolean value);

    CreateBuilder set(GlobField field, Glob value);

    CreateBuilder set(GlobArrayField field, Glob[] values);

    CreateBuilder set(GlobUnionField field, Glob value);

    CreateBuilder set(GlobArrayUnionField field, Glob[] values);

    CreateBuilder set(IntegerField field, IntegerAccessor accessor);

    CreateBuilder set(LongField field, LongAccessor accessor);

    CreateBuilder set(StringField field, StringAccessor accessor);

    CreateBuilder set(DoubleField field, DoubleAccessor accessor);

    CreateBuilder set(BooleanField field, BooleanAccessor accessor);

    CreateBuilder set(BlobField field, BlobAccessor accessor);

    CreateBuilder set(GlobField field, GlobAccessor accessor);

    CreateBuilder set(GlobArrayField field, GlobsAccessor accessor);

    CreateBuilder set(GlobUnionField field, GlobAccessor accessor);

    CreateBuilder set(GlobArrayUnionField field, GlobsAccessor accessor);

    CreateBuilder setObject(Field field, Accessor accessor);

    CreateBuilder setObject(Field field, Object value);

    SqlRequest getRequest();

    BulkDbRequest getBulkRequest();

}
