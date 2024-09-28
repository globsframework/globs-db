package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.Annotations;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeLoaderFactory;
import org.globsframework.core.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.Key;

import java.util.Optional;

public class DbTableName {
    public static GlobType TYPE;

    public static StringField NAME;

    @InitUniqueKey
    public static Key UNIQUE_KEY;

    static {
        GlobTypeLoaderFactory.create(DbTableName.class, "DbTableName")
                .register(GlobCreateFromAnnotation.class, annotation -> create(((DbTableName_) annotation).value()))
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
