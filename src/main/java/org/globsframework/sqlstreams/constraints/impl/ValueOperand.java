package org.globsframework.sqlstreams.constraints.impl;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.sqlstreams.constraints.Operand;
import org.globsframework.sqlstreams.constraints.OperandVisitor;

public class ValueOperand implements Operand {
    private Field field;
    private Object value;

    public ValueOperand(Field field, Object value) {
        this.field = field;
        this.value = value;
    }

    public <T extends OperandVisitor> T visitOperand(T visitor) {
        visitor.visitValueOperand(this);
        return visitor;
    }

    public Field getField() {
        return field;
    }

    public Object getValue() {
        return value;
    }
}
