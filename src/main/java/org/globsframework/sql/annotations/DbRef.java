package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.annotations.FieldName_;
import org.globsframework.core.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;
import org.globsframework.core.model.MutableGlob;

import java.lang.annotation.Annotation;

public class DbRef {
    public static final GlobType TYPE;

    @FieldName_("to")
    public static final StringField TO;

    @InitUniqueKey
    public static final Key KEY;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("DbRef");
        TYPE = typeBuilder.unCompleteType();
        TO = typeBuilder.declareStringField("to");
        typeBuilder.complete();
        typeBuilder.register(GlobCreateFromAnnotation.class, DbRef::create);
        KEY = KeyBuilder.newEmptyKey(TYPE);
//        GlobTypeLoaderFactory.create(DbRef.class, "DbRef")
//                .register(GlobCreateFromAnnotation.class, DbRef::create)
//                .load();
    }

    private static MutableGlob create(Annotation annotation) {
        return TYPE.instantiate().set(TO, ((DbRef_) annotation).to());
    }

}
