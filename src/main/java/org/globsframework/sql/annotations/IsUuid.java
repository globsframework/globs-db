package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;

/**
 * A StringField holding a UUID, stored in the database's own type where it has one — {@code uuid} on
 * PostgreSQL, a fixed 36 character column elsewhere. The field stays a String on the Java side; only
 * the column type and the way the value is bound change.
 */
public class IsUuid {
    public static final GlobType TYPE;

    public static final Key KEY;

    public static final Glob UNIQUE;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("IsUuid");
        TYPE = typeBuilder.build();
        KEY = KeyBuilder.newEmptyKey(TYPE);
        UNIQUE = TYPE.instantiate();
    }
}
