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
        TYPE = typeBuilder.unCompleteType();
        typeBuilder.complete();
        KEY = KeyBuilder.newEmptyKey(TYPE);
        UNIQUE_GLOB = TYPE.instantiate();
        typeBuilder.register(GlobCreateFromAnnotation.class, annotation -> UNIQUE_GLOB);
//        GlobTypeLoaderFactory.create(IsDbKey.class, "IsDbKey")
//                .register(GlobCreateFromAnnotation.class, annotation -> UNIQUE_GLOB)
//                .load();
    }
}
