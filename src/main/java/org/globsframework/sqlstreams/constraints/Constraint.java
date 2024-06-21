package org.globsframework.sqlstreams.constraints;

public interface Constraint {
    <T extends ConstraintVisitor> T accept(T constraintVisitor);
}
