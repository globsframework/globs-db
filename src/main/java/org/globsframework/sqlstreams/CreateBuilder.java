package org.globsframework.sqlstreams;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.fields.BlobField;
import org.globsframework.metamodel.fields.IntegerField;
import org.globsframework.metamodel.fields.LongField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.streams.accessors.*;

public interface CreateBuilder {
    CreateBuilder set(IntegerField field, Integer value);

    CreateBuilder set(BlobField field, byte[] value);

    CreateBuilder set(StringField field, String value);

    CreateBuilder set(LongField field, Long value);

    CreateBuilder set(IntegerField field, IntegerAccessor accessor);

    CreateBuilder set(LongField field, LongAccessor accessor);

    CreateBuilder set(StringField field, StringAccessor accessor);

    CreateBuilder set(BlobField field, BlobAccessor accessor);

    CreateBuilder setObject(Field field, Accessor accessor);

    CreateBuilder setObject(Field field, Object value);

    SqlRequest getRequest();
}
