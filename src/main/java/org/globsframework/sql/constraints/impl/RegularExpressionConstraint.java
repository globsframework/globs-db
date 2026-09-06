package org.globsframework.sql.constraints.impl;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.TableRef;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.ConstraintVisitor;

public class RegularExpressionConstraint implements Constraint {
    public final Field field;
    public final String value;
    public final boolean caseSensitive;
    public final boolean not;
    public final TableRef table;

    public RegularExpressionConstraint(Field field, String value, boolean caseSensitive, boolean not) {
        this(field, null, value, caseSensitive, not);
    }

    public RegularExpressionConstraint(Field field, TableRef table, String value, boolean caseSensitive, boolean not) {
        this.field = field;
        this.table = table;
        this.value = value;
        this.caseSensitive = caseSensitive;
        this.not = not;
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitRegularExpression(field, table, value, caseSensitive, not);
        return visitor;
    }
}
