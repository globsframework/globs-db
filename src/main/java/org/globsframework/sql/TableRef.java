package org.globsframework.sql;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;

import java.util.Objects;

/**
 * One occurrence of a table in a query.
 * <p>
 * A {@code GlobType} says which table; a TableRef says <em>which appearance of it</em>, which is what
 * an alias is for. Two refs on the same type are two aliases, so a type can be joined to itself.
 * <p>
 * Obtained from the builder — {@code rootTable()} for the type the query was opened on,
 * {@code table(type)} for each further occurrence — and turned into a column with
 * {@link #column(Field)}.
 */
public final class TableRef {
    private final GlobType type;
    private final String alias;

    public TableRef(GlobType type, String alias) {
        this.type = Objects.requireNonNull(type, "type");
        this.alias = Objects.requireNonNull(alias, "alias");
    }

    public GlobType getType() {
        return type;
    }

    public String getAlias() {
        return alias;
    }

    public ColumnRef column(Field field) {
        if (field.getGlobType() != type) {
            throw new IllegalArgumentException(field.getFullName() + " does not belong to " + type.getName());
        }
        return new ColumnRef(this, field);
    }

    public String toString() {
        return type.getName() + " as " + alias;
    }
}
