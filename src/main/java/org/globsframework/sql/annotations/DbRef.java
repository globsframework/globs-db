package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeLoaderFactory;
import org.globsframework.core.metamodel.annotations.FieldName_;
import org.globsframework.core.metamodel.annotations.GlobCreateFromAnnotation;
import org.globsframework.core.metamodel.annotations.InitUniqueKey;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.MutableGlob;

import java.lang.annotation.Annotation;

public class DbRef {
    public static GlobType TYPE;

    @FieldName_("to")
    public static StringField TO;

    @InitUniqueKey
    public static Key KEY;

    static {
        GlobTypeLoaderFactory.create(DbRef.class, "DbRef")
                .register(GlobCreateFromAnnotation.class, DbRef::create)
                .load();
    }

    private static MutableGlob create(Annotation annotation) {
        return TYPE.instantiate().set(TO, ((DbRef_) annotation).to());
    }

}
