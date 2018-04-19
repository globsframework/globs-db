package org.globsframework.sqlstreams.constraints;

public interface Constraint {
    <T extends ConstraintVisitor> T visit(T constraintVisitor);
}
