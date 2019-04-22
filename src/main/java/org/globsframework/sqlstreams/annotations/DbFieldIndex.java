package org.globsframework.sqlstreams.annotations;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.metamodel.annotations.InitUniqueKey;
import org.globsframework.metamodel.fields.IntegerField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.model.Key;
import org.globsframework.model.MutableGlob;
import org.globsframework.sqlstreams.annotations.typed.TypedDbFieldName;

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
