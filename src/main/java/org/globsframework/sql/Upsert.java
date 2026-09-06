package org.globsframework.sql;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;

import java.util.List;

/**
 * What an insert should do when the row is already there.
 * <p>
 * Every dialect can express it, none of them the same way: PostgreSQL with {@code ON CONFLICT},
 * MySQL with {@code ON DUPLICATE KEY UPDATE}, HSQLDB and Oracle with a {@code MERGE}. They all end
 * up with one placeholder per inserted column, in the same order, so the values are bound exactly as
 * for a plain insert.
 *
 * @param conflictColumns the columns whose duplication is the conflict, the key fields when left
 *                        empty. MySQL ignores them: {@code ON DUPLICATE KEY} fires on any unique key
 * @param columnsToUpdate what to overwrite on the existing row, every other inserted column when
 *                        left null
 * @param doNothing       true to leave the existing row alone rather than update it
 */
public record Upsert(List<Field> conflictColumns, List<Field> columnsToUpdate, boolean doNothing) {

    public static Upsert update(List<Field> conflictColumns, List<Field> columnsToUpdate) {
        return new Upsert(List.copyOf(conflictColumns), columnsToUpdate == null ? null : List.copyOf(columnsToUpdate), false);
    }

    public static Upsert doNothing(List<Field> conflictColumns) {
        return new Upsert(List.copyOf(conflictColumns), List.of(), true);
    }

    /**
     * Fills in the defaults now that the inserted columns are known: the type's key fields as the
     * conflict target, and every other inserted column as the update list. An upsert whose update
     * list comes out empty — every inserted column being part of the conflict — is a do-nothing.
     */
    public Upsert resolve(GlobType globType, List<Field> inserted) {
        List<Field> conflict = conflictColumns.isEmpty() ? List.of(globType.getKeyFields()) : conflictColumns;
        if (conflict.isEmpty()) {
            throw new IllegalStateException(globType.getName() + " has no key field: name the conflict "
                                            + "columns explicitly");
        }
        for (Field field : conflict) {
            if (!inserted.contains(field)) {
                throw new IllegalStateException(field.getFullName() + " is a conflict column but is not "
                                                + "one of the inserted columns");
            }
        }
        List<Field> toUpdate = (doNothing ? List.<Field>of() : columnsToUpdate != null ? columnsToUpdate
                : inserted.stream().filter(field -> !conflict.contains(field)).toList());
        return new Upsert(conflict, toUpdate, doNothing || toUpdate.isEmpty());
    }
}
