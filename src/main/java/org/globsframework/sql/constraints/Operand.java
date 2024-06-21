package org.globsframework.sql.constraints;

public interface Operand {
    <T extends OperandVisitor> T visitOperand(T visitor);
}
