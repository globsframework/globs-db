package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;
import org.globsframework.core.model.MutableGlob;

/**
 * The column type to write in the DDL, verbatim — the escape hatch for a type this library does not
 * know: {@code inet}, {@code citext}, {@code geometry(Point,4326)}, a domain of your own.
 * <p>
 * On a StringField the value is bound the way {@link IsUuid} and {@link DbJson} are, letting the
 * database read the string as whatever the column actually is. Nothing checks that the type exists
 * or that the dialect in use has it: it goes straight into the CREATE TABLE.
 */
public class DbColumnType {
    public static final GlobType TYPE;

    public static final StringField NAME;

    public static final Key KEY;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("DbColumnType");
        NAME = typeBuilder.declareStringField("name");
        TYPE = typeBuilder.build();
        KEY = KeyBuilder.newEmptyKey(TYPE);
    }

    public static MutableGlob create(String name) {
        return TYPE.instantiate().set(NAME, name);
    }
}
