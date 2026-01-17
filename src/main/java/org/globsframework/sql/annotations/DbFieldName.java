package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;
import org.globsframework.core.model.MutableGlob;

import java.util.Optional;

public class DbFieldName {
    public static final GlobType TYPE;

    public static final StringField NAME;

    @InitUniqueKey
    public static final Key KEY;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("DbFieldName");
        NAME = typeBuilder.declareStringField("name");
        typeBuilder.register(GlobCreateFromAnnotation.class, annotation -> create((DbFieldName_) annotation));
        TYPE = typeBuilder.build();
        KEY = KeyBuilder.newEmptyKey(TYPE);
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
