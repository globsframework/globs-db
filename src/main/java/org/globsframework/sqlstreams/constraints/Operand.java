package org.globsframework.sqlstreams.constraints;

public interface Operand {
    <T extends OperandVisitor> T visitOperand(T visitor);
}
