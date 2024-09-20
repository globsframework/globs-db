package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeLoaderFactory;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.IntegerField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.MutableGlob;

public class DbFieldIndex {
    public static GlobType TYPE;

    public static IntegerField INDEX;

    @InitUniqueKey
    public static Key KEY;

    static {
        GlobTypeLoaderFactory.create(DbFieldIndex.class, "DbFieldIndex")
                .load();
    }

    public static MutableGlob create(int fieldIndex) {
        return TYPE.instantiate().set(INDEX, fieldIndex);
    }

}
