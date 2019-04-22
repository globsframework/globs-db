package org.globsframework.sqlstreams.annotations;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.InitUniqueKey;
import org.globsframework.metamodel.fields.IntegerField;
import org.globsframework.model.Key;
import org.globsframework.model.MutableGlob;

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
