package org.globsframework.sql.constraints.impl;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.TableRef;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.ConstraintVisitor;

public class NullOrNotConstraint implements Constraint {
    private final Field field;
    private final TableRef table;
    private final Boolean checkNull;

    public NullOrNotConstraint(Field field, Boolean checkNull) {
        this(field, null, checkNull);
    }

    public NullOrNotConstraint(Field field, TableRef table, Boolean checkNull) {
        this.field = field;
        this.table = table;
        this.checkNull = checkNull;
    }

    public TableRef getTable() {
        return table;
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitIsOrNotNull(this);
        return visitor;
    }

    public Field getField() {
        return field;
    }

    public boolean checkNull() {
        return checkNull;
    }
}
