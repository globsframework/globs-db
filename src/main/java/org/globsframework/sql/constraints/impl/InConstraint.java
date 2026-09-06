package org.globsframework.sql.constraints.impl;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.TableRef;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.ConstraintVisitor;

import java.util.Set;

public class InConstraint implements Constraint {
    private Field field;
    private final TableRef table;
    private Set<?> values;

    public InConstraint(Field field, Set<?> values) {
        this(field, null, values);
    }

    public InConstraint(Field field, TableRef table, Set<?> values) {
        this.field = field;
        this.table = table;
        this.values = values;
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitIn(this);
        return visitor;
    }

    public Field getField() {
        return field;
    }

    public TableRef getTable() {
        return table;
    }

    public Set<?> getValues() {
        return values;
    }
}
