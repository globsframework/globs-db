package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.IntegerField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;
import org.globsframework.core.model.MutableGlob;

public class DbFieldIndex {
    public static final GlobType TYPE;

    public static final IntegerField INDEX;

    @InitUniqueKey
    public static final Key KEY;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("DbFieldIndex");
        TYPE = typeBuilder.unCompleteType();
        INDEX = typeBuilder.declareIntegerField("index");
        typeBuilder.complete();
        KEY = KeyBuilder.newEmptyKey(TYPE);
//        GlobTypeLoaderFactory.create(DbFieldIndex.class, "DbFieldIndex")
//                .load();
    }

    public static MutableGlob create(int fieldIndex) {
        return TYPE.instantiate().set(INDEX, fieldIndex);
    }

}
