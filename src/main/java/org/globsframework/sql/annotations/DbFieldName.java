package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;
import org.globsframework.core.model.MutableGlob;

import java.util.Optional;

public class DbFieldName {
    public static final GlobType TYPE;

    public static final StringField NAME;

    public static final Key KEY;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("DbFieldName");
        NAME = typeBuilder.declareStringField("name");
        TYPE = typeBuilder.build();
        KEY = KeyBuilder.newEmptyKey(TYPE);
    }

    public static Optional<String> getOptName(Field field) {
        return field.hasAnnotation(KEY) ? Optional.of(field.getAnnotation(KEY).get(NAME)) : Optional.empty();
    }

    public static MutableGlob create(String name) {
        return TYPE.instantiate().set(NAME, name);
    }
}
