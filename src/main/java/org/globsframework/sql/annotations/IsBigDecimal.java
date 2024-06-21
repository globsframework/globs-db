package org.globsframework.sql.annotations;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.metamodel.annotations.InitUniqueGlob;
import org.globsframework.metamodel.annotations.InitUniqueKey;
import org.globsframework.model.Glob;
import org.globsframework.model.Key;

public class IsBigDecimal {
    public static GlobType TYPE;

    @InitUniqueKey
    public static Key KEY;

    @InitUniqueGlob
    public static Glob UNIQUE;

    static {
        GlobTypeLoaderFactory.create(IsBigDecimal.class)
              .register(GlobCreateFromAnnotation.class, annotation -> UNIQUE)
              .load();
    }
}
