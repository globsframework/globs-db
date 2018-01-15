package org.globsframework.sqlstreams.constraints.impl;

import org.globsframework.metamodel.Field;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.ConstraintVisitor;

import java.util.List;

public class NotInConstraint implements Constraint {
    private Field field;
    private List values;

    public NotInConstraint(Field field, List values) {
        this.field = field;
        this.values = values;
    }

    public void visit(ConstraintVisitor constraintVisitor) {
        constraintVisitor.visitNotIn(this);
    }

    public Field getField() {
        return field;
    }

    public List getValues() {
        return values;
    }
}
