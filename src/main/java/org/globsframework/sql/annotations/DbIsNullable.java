package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.BooleanField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;
import org.globsframework.core.model.MutableGlob;

public class DbIsNullable {
    public static final GlobType TYPE;

    public static final BooleanField IS_NULLABLE;

    @InitUniqueKey
    public static final Key KEY;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("DbIsNullable");
        TYPE = typeBuilder.unCompleteType();
        IS_NULLABLE = typeBuilder.declareBooleanField("isNullable");
        typeBuilder.complete();
        typeBuilder.register(GlobCreateFromAnnotation.class, annotation -> create(true));
        KEY = KeyBuilder.newEmptyKey(TYPE);
//        GlobTypeLoaderFactory.create(DbIsNullable.class, "DbIsNullable")
//                .register(GlobCreateFromAnnotation.class, annotation -> create(true))
//                .load();
    }

    public static MutableGlob create(boolean isNullable) {
        return TYPE.instantiate().set(IS_NULLABLE, isNullable);
    }

}
