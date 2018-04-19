package org.globsframework.sqlstreams.constraints.impl;

import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.ConstraintVisitor;

public class AndConstraint extends BinaryConstraint implements Constraint {

    public AndConstraint(Constraint leftOperand, Constraint rightOperand) {
        super(leftOperand, rightOperand);
    }

    public <T extends ConstraintVisitor> T visit(T visitor) {
        visitor.visitAnd(this);
        return visitor;
    }
}
