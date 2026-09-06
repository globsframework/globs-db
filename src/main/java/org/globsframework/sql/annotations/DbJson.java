package org.globsframework.sql.annotations;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.Key;
import org.globsframework.core.model.KeyBuilder;

/**
 * Store this column in the database's own JSON type — {@code jsonb} on PostgreSQL, {@code JSON} on
 * MySQL — instead of the long text column used otherwise.
 * <p>
 * It applies to a StringField already holding JSON, and to the composite fields this library encodes
 * as JSON itself: GlobField, GlobArrayField and their union forms. What changes is the column type
 * and the binding; the value read back is the same string either way, so nothing above this layer
 * moves. Opt-in: without the annotation a composite field stays in a long text column, as before.
 */
public class DbJson {
    public static final GlobType TYPE;

    public static final Key KEY;

    public static final Glob UNIQUE;

    static {
        GlobTypeBuilder typeBuilder = GlobTypeBuilderFactory.create("DbJson");
        TYPE = typeBuilder.build();
        KEY = KeyBuilder.newEmptyKey(TYPE);
        UNIQUE = TYPE.instantiate();
    }
}
