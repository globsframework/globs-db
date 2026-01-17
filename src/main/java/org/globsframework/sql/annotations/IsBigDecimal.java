package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.core.metamodel.annotations.InitUniqueGlob;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;

public class IsBigDecimal {
    public static final GlobType TYPE;

    @InitUniqueKey
    public static final Key KEY;

    @InitUniqueGlob
    public static final Glob UNIQUE;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("IsBigDecimal");
        typeBuilder.register(GlobCreateFromAnnotation.class, annotation -> getUnique());
        TYPE = typeBuilder.build();
        KEY = KeyBuilder.newEmptyKey(TYPE);
        UNIQUE = TYPE.instantiate();
    }

    private static Glob getUnique() {
        return UNIQUE;
    }
}
