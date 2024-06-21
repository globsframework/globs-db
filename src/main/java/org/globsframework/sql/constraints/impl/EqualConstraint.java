package org.globsframework.sql.constraints.impl;

import org.globsframework.sql.constraints.ConstraintVisitor;
import org.globsframework.sql.constraints.Operand;

public class EqualConstraint extends BinaryOperandConstraint {

    public EqualConstraint(Operand leftOp, Operand rightOp) {
        super(leftOp, rightOp);
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitEqual(this);
        return visitor;
    }

}
