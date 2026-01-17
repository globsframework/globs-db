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

public class IsDbKey {
    public static final GlobType TYPE;

    @InitUniqueKey
    public static final Key KEY;

    @InitUniqueGlob
    public static final Glob UNIQUE_GLOB;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("IsDbKey");
        typeBuilder.register(GlobCreateFromAnnotation.class, annotation -> getUniqueGlob());
        TYPE = typeBuilder.build();
        KEY = KeyBuilder.newEmptyKey(TYPE);
        UNIQUE_GLOB = TYPE.instantiate();
    }

    private static Glob getUniqueGlob() {
        return UNIQUE_GLOB;
    }
}
