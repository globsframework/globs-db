package org.globsframework.sql.annotations;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.InitUniqueKey;
import org.globsframework.metamodel.fields.IntegerField;
import org.globsframework.model.Key;
import org.globsframework.model.MutableGlob;

public class DbFieldNumericPrecision {
    public static GlobType TYPE;

    public static IntegerField SIZE;

    @InitUniqueKey
    public static Key KEY;

    static {
        GlobTypeLoaderFactory.create(DbFieldNumericPrecision.class, "DbFieldNumericPrecision")
              .load();
    }

    public static MutableGlob create(int size) {
        return TYPE.instantiate().set(SIZE, size);
    }

}
