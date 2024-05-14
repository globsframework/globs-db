package org.globsframework.sqlstreams.constraints;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.sqlstreams.constraints.impl.AccessorOperand;
import org.globsframework.sqlstreams.constraints.impl.ValueOperand;

public interface OperandVisitor {
    void visitValueOperand(ValueOperand value);

    void visitAccessorOperand(AccessorOperand accessorOperand);

    void visitFieldOperand(Field field);
}
