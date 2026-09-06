package org.globsframework.sql.constraints;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.TableRef;
import org.globsframework.sql.constraints.impl.AccessorOperand;
import org.globsframework.sql.constraints.impl.ValueOperand;

public interface OperandVisitor {
    void visitValueOperand(ValueOperand value);

    void visitAccessorOperand(AccessorOperand accessorOperand);

    void visitFieldOperand(Field field);

    /**
     * @param table the occurrence the column belongs to, null when the constraint names a bare
     *              field — which resolves to the query's only occurrence of its type
     */
    default void visitFieldOperand(Field field, TableRef table) {
        visitFieldOperand(field);
    }
}
