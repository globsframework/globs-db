package org.globsframework.sql;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.metamodel.fields.*;
import org.globsframework.streams.accessors.*;
import org.globsframework.utils.Ref;

public interface SelectBuilder {

    SelectQuery getQuery();

    SelectQuery getQuery(String sql);

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


    SelectBuilder select(LongArrayField field, Ref<LongArrayAccessor> accessor);

    SelectBuilder groupBy(Field field);

    SelectBuilder orderAsc(Field field);

    SelectBuilder orderDesc(Field field);

    SelectBuilder top(int n);

    SelectBuilder skip(int n);

    SelectBuilder withKeys();

    IntegerAccessor max(IntegerField field);

    LongAccessor max(LongField field);

    LongAccessor count(Field field);

    default LongAccessor count(IntegerField field) {
        return count(((Field) field));
    }

    default LongAccessor count(LongField field) {
        return count(((Field) field));
    };

    LongAccessor sum(IntegerField count);

    LongAccessor sum(LongField field);

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

    StringArrayAccessor retrieve(StringArrayField field);

    IntegerArrayAccessor retrieve(IntegerArrayField field);

    LongArrayAccessor retrieve(LongArrayField field);

    DoubleArrayAccessor retrieve(DoubleArrayField field);

    BooleanArrayAccessor retrieve(BooleanArrayField field);

    GlobAccessor retrieve(GlobField field);

    GlobAccessor retrieve(GlobUnionField field);

    GlobsAccessor retrieve(GlobArrayField field);

    GlobsAccessor retrieve(GlobArrayUnionField field);

    Accessor retrieveUnTyped(Field field);

}
