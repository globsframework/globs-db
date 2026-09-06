package org.globsframework.sql.constraints.impl;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.SubQuery;
import org.globsframework.sql.TableRef;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.ConstraintVisitor;

/**
 * {@code column IN (SELECT ... FROM ... WHERE ...)}, and its negation — the form of {@code in} whose
 * values are not known when the query is built.
 */
public class InSubQueryConstraint implements Constraint {
    private final Field field;
    private final TableRef table;
    private final SubQuery subQuery;
    private final boolean not;

    public InSubQueryConstraint(Field field, TableRef table, SubQuery subQuery, boolean not) {
        this.field = field;
        this.table = table;
        this.subQuery = subQuery;
        this.not = not;
    }

    public Field getField() {
        return field;
    }

    public TableRef getTable() {
        return table;
    }

    public SubQuery getSubQuery() {
        return subQuery;
    }

    public boolean isNot() {
        return not;
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitInSubQuery(this);
        return visitor;
    }
}
