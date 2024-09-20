package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeLoaderFactory;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.IntegerField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.MutableGlob;

public class DbFieldSqlType {
    public static GlobType TYPE;

    public static IntegerField SQL_TYPE;

    @InitUniqueKey
    public static Key KEY;

    static {
        GlobTypeLoaderFactory.create(DbFieldSqlType.class, "DbFieldSqlType")
                .load();
    }

    public static MutableGlob create(int sqlFieldType) {
        return TYPE.instantiate().set(SQL_TYPE, sqlFieldType);
    }

}
