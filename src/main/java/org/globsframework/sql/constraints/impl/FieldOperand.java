package org.globsframework.sql.constraints.impl;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.constraints.Operand;
import org.globsframework.sql.constraints.OperandVisitor;

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
