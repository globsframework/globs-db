package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;

public class IsBigDecimal {
    public static final GlobType TYPE;

    public static final Key KEY;
    public static final Glob UNIQUE;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("IsBigDecimal");
        TYPE = typeBuilder.build();
        KEY = KeyBuilder.newEmptyKey(TYPE);
        UNIQUE = TYPE.instantiate();
    }
}
