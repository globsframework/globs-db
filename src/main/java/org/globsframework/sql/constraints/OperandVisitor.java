package org.globsframework.sql.constraints;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.constraints.impl.AccessorOperand;
import org.globsframework.sql.constraints.impl.ValueOperand;

public interface OperandVisitor {
    void visitValueOperand(ValueOperand value);

    void visitAccessorOperand(AccessorOperand accessorOperand);

    void visitFieldOperand(Field field);
}
