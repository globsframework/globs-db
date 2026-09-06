package org.globsframework.sql.constraints.impl;

import org.globsframework.sql.SubQuery;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.ConstraintVisitor;

/**
 * {@code EXISTS (SELECT 1 FROM ... WHERE ...)}, and its negation.
 */
public class ExistsConstraint implements Constraint {
    private final SubQuery subQuery;
    private final boolean not;

    public ExistsConstraint(SubQuery subQuery, boolean not) {
        this.subQuery = subQuery;
        this.not = not;
    }

    public SubQuery getSubQuery() {
        return subQuery;
    }

    public boolean isNot() {
        return not;
    }

    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitExists(this);
        return visitor;
    }
}
