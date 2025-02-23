package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.Annotations;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;

import java.util.Optional;

public class DbTableName {
    public static final GlobType TYPE;

    public static final StringField NAME;

    @InitUniqueKey
    public static final Key UNIQUE_KEY;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("DbTableName");
        TYPE = typeBuilder.unCompleteType();
        NAME = typeBuilder.declareStringField("name");
        typeBuilder.complete();
        typeBuilder.register(GlobCreateFromAnnotation.class, annotation -> create(((DbTableName_) annotation).value()));
        UNIQUE_KEY = KeyBuilder.newEmptyKey(TYPE);

//        GlobTypeLoaderFactory.create(DbTableName.class, "DbTableName")
//                .register(GlobCreateFromAnnotation.class, annotation -> create(((DbTableName_) annotation).value()))
//                .load();
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
