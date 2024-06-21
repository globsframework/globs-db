package org.globsframework.sql.constraints.impl;

import org.globsframework.sql.constraints.ConstraintVisitor;
import org.globsframework.sql.constraints.Operand;

public class StrictlyBiggerThanConstraint extends BinaryOperandConstraint {
    public StrictlyBiggerThanConstraint(Operand leftOperand, Operand rightOperand) {
        super(leftOperand, rightOperand);
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitStrictlyBiggerThan(this);
        return visitor;
    }
}
