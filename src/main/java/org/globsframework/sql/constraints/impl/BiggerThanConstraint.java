package org.globsframework.sql.constraints.impl;

import org.globsframework.sql.constraints.ConstraintVisitor;
import org.globsframework.sql.constraints.Operand;

public class BiggerThanConstraint extends BinaryOperandConstraint {
    public BiggerThanConstraint(Operand leftOperand, Operand rightOperand) {
        super(leftOperand, rightOperand);
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitBiggerThan(this);
        return visitor;
    }
}
