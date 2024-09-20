package org.globsframework.sql.constraints.impl;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.streams.accessors.Accessor;
import org.globsframework.sql.constraints.Operand;
import org.globsframework.sql.constraints.OperandVisitor;

public class AccessorOperand implements Operand {
    private Field field;
    private Accessor accessor;

    public AccessorOperand(Field field, Accessor accessor) {
        this.field = field;
        this.accessor = accessor;
    }

    public <T extends OperandVisitor> T visitOperand(T visitor) {
        visitor.visitAccessorOperand(this);
        return visitor;
    }

    public Accessor getAccessor() {
        return accessor;
    }

    public Field getField() {
        return field;
    }
}
