package org.globsframework.sqlstreams.annotations;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.FieldNameAnnotation;
import org.globsframework.metamodel.annotations.InitUniqueKey;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.model.Key;

public class IsDbKey {
    public static GlobType TYPE;

    @FieldNameAnnotation("to")
    public static StringField TO;

    @InitUniqueKey
    public static Key KEY;

    static {
        GlobTypeLoaderFactory.create(IsDbKey.class, "IsDbKey")
              .load();
    }

}
