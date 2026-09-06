package org.globsframework.sql.constraints;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.TableRef;
import org.globsframework.sql.constraints.impl.*;

public interface ConstraintVisitor {
    void visitEqual(EqualConstraint constraint);

    void visitNotEqual(NotEqualConstraint constraint);

    void visitAnd(AndConstraint constraint);

    void visitOr(OrConstraint constraint);

    void visitLessThan(LessThanConstraint constraint);

    void visitBiggerThan(BiggerThanConstraint constraint);

    void visitStrictlyBiggerThan(StrictlyBiggerThanConstraint constraint);

    void visitStrictlyLesserThan(StrictlyLesserThanConstraint constraint);

    void visitIn(InConstraint constraint);

    void visitIsOrNotNull(NullOrNotConstraint constraint);

    void visitNotIn(NotInConstraint constraint);

    enum ContainType {
        startWith, endWith, contains
    }

    void visitContains(Field field, String value, ContainType containType, boolean contains, boolean ignoreCase);

    void visitRegularExpression(Field field, String value, boolean caseInsensitive, boolean not);

    /**
     * @param table the occurrence the column belongs to, null when the constraint names a bare field
     */
    default void visitContains(Field field, TableRef table, String value, ContainType containType,
                               boolean contains, boolean ignoreCase) {
        visitContains(field, value, containType, contains, ignoreCase);
    }

    default void visitRegularExpression(Field field, TableRef table, String value, boolean caseInsensitive,
                                        boolean not) {
        visitRegularExpression(field, value, caseInsensitive, not);
    }
}
