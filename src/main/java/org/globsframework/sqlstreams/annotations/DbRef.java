package org.globsframework.sqlstreams.annotations;

import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeLoaderFactory;
import org.globsframework.metamodel.annotations.FieldNameAnnotation;
import org.globsframework.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.metamodel.annotations.InitUniqueKey;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.model.Key;
import org.globsframework.model.MutableGlob;
import org.globsframework.sqlstreams.annotations.typed.TypedDbRef;

import java.lang.annotation.Annotation;

public class DbRef {
    public static GlobType TYPE;

    @FieldNameAnnotation("to")
    public static StringField TO;

    @InitUniqueKey
    public static Key KEY;

    static {
        GlobTypeLoaderFactory.create(DbRef.class, "DbRef")
              .register(GlobCreateFromAnnotation.class, DbRef::create)
              .load();
    }

    private static MutableGlob create(Annotation annotation) {
        return TYPE.instantiate().set(TO, ((TypedDbRef) annotation).to());
    }

}
