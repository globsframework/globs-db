package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.IntegerField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;
import org.globsframework.core.model.MutableGlob;

public class DbSqlType {
    public static final GlobType TYPE;

    public static final IntegerField SQL_TYPE;

    @InitUniqueKey
    public static final Key UNIQUE_KEY;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("DbSqlType");
        TYPE = typeBuilder.unCompleteType();
        SQL_TYPE = typeBuilder.declareIntegerField("sqlType");
        typeBuilder.complete();
        UNIQUE_KEY = KeyBuilder.newEmptyKey(TYPE);

//        GlobTypeLoaderFactory.create(DbSqlType.class, "DbSqlType")
//                .load();
    }

    public static MutableGlob create(int sqlFieldType) {
        return TYPE.instantiate().set(SQL_TYPE, sqlFieldType);
    }

}
