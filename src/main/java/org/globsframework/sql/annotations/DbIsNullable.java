package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeLoaderFactory;
import org.globsframework.core.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.BooleanField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.MutableGlob;

public class DbIsNullable {
    public static GlobType TYPE;

    public static BooleanField IS_NULLABLE;

    @InitUniqueKey
    public static Key KEY;

    static {
        GlobTypeLoaderFactory.create(DbIsNullable.class, "DbIsNullable")
                .register(GlobCreateFromAnnotation.class, annotation -> create(true))
                .load();
    }

    public static MutableGlob create(boolean isNullable) {
        return TYPE.instantiate().set(IS_NULLABLE, isNullable);
    }

}
