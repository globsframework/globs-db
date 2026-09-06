package org.globsframework.sql.constraints.impl;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.TableRef;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.ConstraintVisitor;
import org.globsframework.sql.constraints.Operand;

/**
 * {@code column BETWEEN min AND max}, bounds included.
 */
public class BetweenConstraint implements Constraint {
    private final Field field;
    private final TableRef table;
    private final Operand min;
    private final Operand max;

    public BetweenConstraint(Field field, TableRef table, Operand min, Operand max) {
        this.field = field;
        this.table = table;
        this.min = min;
        this.max = max;
    }

    public Field getField() {
        return field;
    }

    public TableRef getTable() {
        return table;
    }

    public Operand getMin() {
        return min;
    }

    public Operand getMax() {
        return max;
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitBetween(this);
        return visitor;
    }
}
