package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.IntegerField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;
import org.globsframework.core.model.MutableGlob;

public class DbMaxCharSize {
    public static final GlobType TYPE;

    public static final IntegerField SIZE;

    @InitUniqueKey
    public static final Key KEY;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("DbMaxCharSize");
        TYPE = typeBuilder.unCompleteType();
        SIZE = typeBuilder.declareIntegerField("size");
        typeBuilder.complete();
        KEY = KeyBuilder.newEmptyKey(TYPE);
//        GlobTypeLoaderFactory.create(DbMaxCharSize.class, "DbMaxCharSize")
//                .load();
    }

    public static MutableGlob create(int size) {
        return TYPE.instantiate().set(SIZE, size);
    }

}
