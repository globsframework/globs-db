package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeLoaderFactory;
import org.globsframework.core.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.core.metamodel.annotations.InitUniqueGlob;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.Key;

public class IsDbKey {
    public static GlobType TYPE;

    @InitUniqueKey
    public static Key KEY;

    @InitUniqueGlob
    public static Glob UNIQUE_GLOB;

    static {
        GlobTypeLoaderFactory.create(IsDbKey.class, "IsDbKey")
                .register(GlobCreateFromAnnotation.class, annotation -> UNIQUE_GLOB)
                .load();
    }
}
