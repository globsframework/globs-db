package org.globsframework.sql.constraints.impl;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.ConstraintVisitor;

public class NullOrNotConstraint implements Constraint {
    private final Field field;
    private final Boolean checkNull;

    public NullOrNotConstraint(Field field, Boolean checkNull) {
        this.field = field;
        this.checkNull = checkNull;
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitIsOrNotNull(this);
        return visitor;
    }

    public Field getField() {
        return field;
    }

    public boolean checkNull() {
        return checkNull;
    }
}
