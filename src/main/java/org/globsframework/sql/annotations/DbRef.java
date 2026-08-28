package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;

public class DbRef {
    public static final GlobType TYPE;

    public static final StringField TO;

    public static final Key KEY;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("DbRef");
        TO = typeBuilder.declareStringField("to");
        TYPE = typeBuilder.build();
        KEY = KeyBuilder.newEmptyKey(TYPE);
    }

}
