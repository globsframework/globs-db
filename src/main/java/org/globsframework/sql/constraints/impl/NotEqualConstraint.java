package org.globsframework.sql.constraints.impl;

import org.globsframework.sql.constraints.ConstraintVisitor;
import org.globsframework.sql.constraints.Operand;

public class NotEqualConstraint extends BinaryOperandConstraint {

    public NotEqualConstraint(Operand left, Operand right) {
        super(left, right);
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitNotEqual(this);
        return visitor;
    }
}
