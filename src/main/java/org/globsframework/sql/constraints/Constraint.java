package org.globsframework.sql.constraints;

public interface Constraint {
    <T extends ConstraintVisitor> T accept(T constraintVisitor);
}
