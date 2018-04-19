package org.globsframework.sqlstreams.annotations;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.metamodel.annotations.InitUniqueGlob;
import org.globsframework.metamodel.annotations.InitUniqueKey;
import org.globsframework.model.Glob;
import org.globsframework.model.Key;

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
