package org.globsframework.sqlstreams.constraints.impl;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.ConstraintVisitor;

public class RegularExpressionConstraint implements Constraint {
    public final Field field;
    public final String value;
    public final boolean caseSensitive;
    public final boolean not;
    public RegularExpressionConstraint(Field field, String value, boolean caseSensitive, boolean not) {
        this.field = field;
        this.value = value;
        this.caseSensitive = caseSensitive;
        this.not = not;
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitRegularExpression(field, value, caseSensitive, not);
        return visitor;
    }
}
