package org.globsframework.sqlstreams.constraints.impl;

import org.globsframework.sqlstreams.constraints.ConstraintVisitor;
import org.globsframework.sqlstreams.constraints.Operand;

public class StrictlyBiggerThanConstraint extends BinaryOperandConstraint {
    public StrictlyBiggerThanConstraint(Operand leftOperand, Operand rightOperand) {
        super(leftOperand, rightOperand);
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitStrictlyBiggerThan(this);
        return visitor;
    }
}
