package org.globsframework.sqlstreams.constraints.impl;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.ConstraintVisitor;

import java.util.Set;

public class InConstraint implements Constraint {
    private Field field;
    private Set values;

    public InConstraint(Field field, Set values) {
        this.field = field;
        this.values = values;
    }

    public <T extends ConstraintVisitor> T visit(T visitor) {
        visitor.visitIn(this);
        return visitor;
    }

    public Field getField() {
        return field;
    }

    public Set getValues() {
        return values;
    }
}
