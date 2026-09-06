package org.globsframework.sql.constraints.impl;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.ColumnRef;
import org.globsframework.sql.TableRef;
import org.globsframework.sql.constraints.Operand;
import org.globsframework.sql.constraints.OperandVisitor;

public class FieldOperand implements Operand {
    private final Field field;
    private final TableRef table;

    public FieldOperand(Field field) {
        this(field, null);
    }

    public FieldOperand(ColumnRef column) {
        this(column.field(), column.table());
    }

    public FieldOperand(Field field, TableRef table) {
        this.field = field;
        this.table = table;
    }

    public Field getField() {
        return field;
    }

    /**
     * The occurrence this column belongs to, null when the operand names a bare field.
     */
    public TableRef getTable() {
        return table;
    }

    public <T extends OperandVisitor> T visitOperand(T visitor) {
        visitor.visitFieldOperand(field, table);
        return visitor;
    }
}
