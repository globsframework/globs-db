package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.BooleanField;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;
import org.globsframework.core.model.MutableGlob;

public class DbIsNullable {
    public static final GlobType TYPE;

    public static final BooleanField IS_NULLABLE;

    public static Glob nullable() {
        return nullable;
    }

    public static Glob notNullable() {
        return notNullable;
    }

    @InitUniqueKey
    public static final Key KEY;

    public static final Glob nullable;

    public static final Glob notNullable;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("DbIsNullable");
        IS_NULLABLE = typeBuilder.declareBooleanField("isNullable");
        typeBuilder.register(GlobCreateFromAnnotation.class, annotation -> create(true));
        TYPE = typeBuilder.build();
        KEY = KeyBuilder.newEmptyKey(TYPE);
        nullable = create(true);
        notNullable = create(false);
    }

    public static MutableGlob create(boolean isNullable) {
        return TYPE.instantiate().set(IS_NULLABLE, isNullable);
    }

}
