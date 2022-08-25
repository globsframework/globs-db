package org.globsframework.sqlstreams.constraints.impl;

import org.globsframework.metamodel.Field;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.ConstraintVisitor;

public class ContainsConstraint implements Constraint {
    public final Field field;
    public final String value;
    public final boolean startWith;
    private boolean contains;

    public ContainsConstraint(Field field, String value, boolean startWith, boolean contains) {
        this.field = field;
        this.value = value;
        this.startWith = startWith;
        this.contains = contains;
    }

    public <T extends ConstraintVisitor> T visit(T visitor) {
        visitor.visitContains(field, value, contains, startWith);
        return visitor;
    }

}
