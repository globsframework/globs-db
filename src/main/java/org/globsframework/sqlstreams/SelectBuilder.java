package org.globsframework.sqlstreams;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.fields.*;
import org.globsframework.streams.accessors.*;
import org.globsframework.utils.Ref;

import java.util.Collection;

// attention sur le distinct : les valeurs de la clef sont automatiquement ajoutees  ==> faire un distinct a part

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

    SelectBuilder select(BlobField field, Ref<BlobAccessor> accessor);

    SelectBuilder orderAsc(Field field);

    SelectBuilder orderDesc(Field field);

    SelectBuilder top(int n);

    SelectBuilder withKeys();

    IntegerAccessor retrieve(IntegerField field);

    LongAccessor retrieve(LongField field);

    StringAccessor retrieve(StringField field);

    BooleanAccessor retrieve(BooleanField field);

    DoubleAccessor retrieve(DoubleField field);

    BlobAccessor retrieve(BlobField field);

    Accessor retrieveUnTyped(Field field);

}
