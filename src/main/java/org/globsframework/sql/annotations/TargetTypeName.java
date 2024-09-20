package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.Annotations;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeLoaderFactory;
import org.globsframework.core.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.Key;
import org.globsframework.sql.annotations.typed.TargetTypeNameAnnotation;

import java.util.Optional;

public class TargetTypeName {
    public static GlobType TYPE;

    public static StringField NAME;

    @InitUniqueKey
    public static Key UNIQUE_KEY;

    static class PHYSICAL_TYPE {
    }

    static {
        GlobTypeLoaderFactory.create(TargetTypeName.class, "PHYSICAL_TYPE")
                .register(GlobCreateFromAnnotation.class, annotation -> create(((TargetTypeNameAnnotation) annotation).value()))
                .load();
    }

    public static Glob get(Annotations annotations) {
        return annotations.getAnnotation(UNIQUE_KEY);
    }

    public static String getName(GlobType type) {
        return type.hasAnnotation(UNIQUE_KEY) ? type.getAnnotation(UNIQUE_KEY).get(NAME) : type.getName();
    }

    public static Optional<String> getOptName(GlobType type) {
        return type.hasAnnotation(UNIQUE_KEY) ? Optional.of(type.getAnnotation(UNIQUE_KEY).get(NAME)) : Optional.empty();
    }

    public static Glob create(String typeName) {
        return TYPE.instantiate().set(NAME, typeName);
    }
}
