package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeLoaderFactory;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.IntegerField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.MutableGlob;

public class DbFieldMaxCharSize {
    public static GlobType TYPE;

    public static IntegerField SIZE;

    @InitUniqueKey
    public static Key KEY;

    static {
        GlobTypeLoaderFactory.create(DbFieldMaxCharSize.class, "DbFieldMaxCharSize")
                .load();
    }

    public static MutableGlob create(int size) {
        return TYPE.instantiate().set(SIZE, size);
    }

}
