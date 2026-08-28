package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;

public class IsDbKey {
    public static final GlobType TYPE;

    public static final Key KEY;
    public static final Glob UNIQUE_GLOB;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("IsDbKey");
        TYPE = typeBuilder.build();
        KEY = KeyBuilder.newEmptyKey(TYPE);
        UNIQUE_GLOB = TYPE.instantiate();
    }
}
