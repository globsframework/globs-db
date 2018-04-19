package org.globsframework.sqlstreams.constraints.impl;

import org.globsframework.sqlstreams.constraints.ConstraintVisitor;
import org.globsframework.sqlstreams.constraints.Operand;

public class EqualConstraint extends BinaryOperandConstraint {

    public EqualConstraint(Operand leftOp, Operand rightOp) {
        super(leftOp, rightOp);
    }

    public <T extends ConstraintVisitor> T visit(T visitor) {
        visitor.visitEqual(this);
        return visitor;
    }

}
