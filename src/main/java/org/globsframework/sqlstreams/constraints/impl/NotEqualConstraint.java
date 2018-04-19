package org.globsframework.sqlstreams.constraints.impl;

import org.globsframework.sqlstreams.constraints.ConstraintVisitor;
import org.globsframework.sqlstreams.constraints.Operand;

public class NotEqualConstraint extends BinaryOperandConstraint {

    public NotEqualConstraint(Operand left, Operand right) {
        super(left, right);
    }

    public <T extends ConstraintVisitor> T visit(T visitor) {
        visitor.visitNotEqual(this);
        return visitor;
    }
}
