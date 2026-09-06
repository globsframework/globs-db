package org.globsframework.sql.constraints.impl;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.TableRef;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.ConstraintVisitor;

public class ContainsConstraint implements Constraint {
    public final Field field;
    public final String value;
    private final ConstraintVisitor.ContainType containType;
    private final boolean contains;
    private final boolean ignoreCase;

    public final TableRef table;

    public ContainsConstraint(Field field, String value, ConstraintVisitor.ContainType containType, boolean contains, boolean ignoreCase) {
        this(field, null, value, containType, contains, ignoreCase);
    }

    public ContainsConstraint(Field field, TableRef table, String value, ConstraintVisitor.ContainType containType, boolean contains, boolean ignoreCase) {
        this.field = field;
        this.table = table;
        this.value = value;
        this.containType = containType;
        this.contains = contains;
        this.ignoreCase = ignoreCase;
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitContains(field, table, value, containType, contains, ignoreCase);
        return visitor;
    }

}
