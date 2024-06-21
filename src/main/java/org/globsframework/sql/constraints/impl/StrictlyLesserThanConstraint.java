package org.globsframework.sql.constraints.impl;

import org.globsframework.sql.constraints.ConstraintVisitor;
import org.globsframework.sql.constraints.Operand;

public class StrictlyLesserThanConstraint extends BinaryOperandConstraint {
    public StrictlyLesserThanConstraint(Operand leftOperand, Operand rightOperand) {
        super(leftOperand, rightOperand);
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitStrictlyLesserThan(this);
        return visitor;
    }
}
