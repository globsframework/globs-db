package org.globsframework.sqlstreams;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.fields.*;
import org.globsframework.streams.accessors.*;
import org.globsframework.utils.Ref;

import java.util.Collection;

public interface SelectBuilder {

    SelectQuery getQuery();

    SelectQuery getNotAutoCloseQuery();

    SelectBuilder select(Field field);

    SelectBuilder selectAll();

    SelectBuilder select(IntegerField field, Ref<IntegerAccessor> accessor);

    SelectBuilder select(LongField field, Ref<LongAccessor> accessor);

    SelectBuilder select(BooleanField field, Ref<BooleanAccessor> accessor);

    SelectBuilder select(StringField field, Ref<StringAccessor> accessor);

    SelectBuilder select(DoubleField field, Ref<DoubleAccessor> accessor);

    SelectBuilder select(DateTimeField field, Ref<DateTimeAccessor> accessor);

    SelectBuilder select(DateField field, Ref<DateAccessor> accessor);

    SelectBuilder select(BlobField field, Ref<BlobAccessor> accessor);

    SelectBuilder select(GlobField field, Ref<GlobAccessor> accessor);

    SelectBuilder select(GlobArrayField field, Ref<GlobsAccessor> accessor);

    SelectBuilder orderAsc(Field field);

    SelectBuilder orderDesc(Field field);

    SelectBuilder top(int n);

    SelectBuilder withKeys();

    IntegerAccessor max(IntegerField field);

    LongAccessor max(LongField field);

    IntegerAccessor min(IntegerField field);

    LongAccessor min(LongField field);

    IntegerAccessor retrieve(IntegerField field);

    LongAccessor retrieve(LongField field);

    StringAccessor retrieve(StringField field);

    BooleanAccessor retrieve(BooleanField field);

    DoubleAccessor retrieve(DoubleField field);

    DateTimeAccessor retrieve(DateTimeField field);

    DateAccessor retrieve(DateField field);

    BlobAccessor retrieve(BlobField field);

    GlobAccessor retrieve(GlobField field);

    GlobsAccessor retrieve(GlobArrayField field);

    Accessor retrieveUnTyped(Field field);

}
