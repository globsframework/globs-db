package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeLoaderFactory;
import org.globsframework.core.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.MutableGlob;

import java.util.Optional;

public class DbFieldName {
    public static GlobType TYPE;

    public static StringField NAME;

    @InitUniqueKey
    public static Key KEY;

    static {
        GlobTypeLoaderFactory.create(DbFieldName.class, "DbFieldName")
                .register(GlobCreateFromAnnotation.class, annotation -> create((DbFieldName_) annotation))
                .load();
    }

    private static MutableGlob create(DbFieldName_ annotation) {
        return TYPE.instantiate().set(NAME, annotation.value());
    }

    public static Optional<String> getOptName(Field field) {
        return field.hasAnnotation(KEY) ? Optional.of(field.getAnnotation(KEY).get(NAME)) : Optional.empty();
    }

    public static MutableGlob create(String name) {
        return TYPE.instantiate().set(NAME, name);
    }
}
