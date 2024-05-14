package org.globsframework.sqlstreams.constraints.impl;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.sqlstreams.constraints.Operand;
import org.globsframework.sqlstreams.constraints.OperandVisitor;

public class FieldOperand implements Operand {
    private Field field;

    public FieldOperand(Field field) {
        this.field = field;
    }

    public <T extends OperandVisitor> T visitOperand(T visitor) {
        visitor.visitFieldOperand(field);
        return visitor;
    }
}
