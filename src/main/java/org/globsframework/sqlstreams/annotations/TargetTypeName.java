package org.globsframework.sqlstreams.annotations;

import org.globsframework.metamodel.Annotations;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.metamodel.annotations.InitUniqueKey;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.model.Glob;
import org.globsframework.model.Key;
import org.globsframework.sqlstreams.annotations.typed.TargetTypeNameAnnotation;

public class TargetTypeName {
    public static GlobType TYPE;

    public static StringField NAME;

    @InitUniqueKey
    public static Key UNIQUE_KEY;

    static class PHYSICAL_TYPE{
    }

    static {
        GlobTypeLoaderFactory.create(TargetTypeName.class, "PHYSICAL_TYPE")
              .register(GlobCreateFromAnnotation.class, annotation -> create(((TargetTypeNameAnnotation)annotation).value()))
              .load();
    }

    public static Glob get(Annotations annotations) {
        return annotations.getAnnotation(UNIQUE_KEY);
    }

    public static String getName(GlobType type) {
        return type.hasAnnotation(UNIQUE_KEY) ? type.getAnnotation(UNIQUE_KEY).get(NAME) : type.getName();
    }

    public static Glob create(String typeName) {
        return TYPE.instantiate().set(NAME, typeName);
    }
}
