package org.globsframework.sql.constraints.impl;

import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.ConstraintVisitor;

/**
 * Negates a whole subtree. Each operator already has its negated form — notEqual, notIn, notContains
 * — but nothing could negate an and/or, so a condition had to be pushed through De Morgan by hand.
 */
public class NotConstraint implements Constraint {
    private final Constraint constraint;

    public NotConstraint(Constraint constraint) {
        this.constraint = constraint;
    }

    public Constraint getConstraint() {
        return constraint;
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitNot(this);
        return visitor;
    }
}
