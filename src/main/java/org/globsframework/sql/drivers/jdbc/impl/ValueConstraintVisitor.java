package org.globsframework.sql.drivers.jdbc.impl;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.utils.exceptions.UnexpectedApplicationState;
import org.globsframework.sql.SubQuery;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.ConstraintVisitor;
import org.globsframework.sql.constraints.OperandVisitor;
import org.globsframework.sql.constraints.impl.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.util.Set;

public class ValueConstraintVisitor extends SqlValueFieldVisitor implements ConstraintVisitor, OperandVisitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ValueConstraintVisitor.class);
    private int index = 0;

    public ValueConstraintVisitor(PreparedStatement preparedStatement) {
        super(preparedStatement);
    }

    public ValueConstraintVisitor(PreparedStatement preparedStatement, int index) {
        super(preparedStatement);
        this.index = index;
    }

    /**
     * How many placeholders have been bound, so that the next clause can carry on from there — a
     * join condition is bound before the WHERE it precedes in the statement.
     */
    public int getIndex() {
        return index;
    }

    public void visitEqual(EqualConstraint constraint) {
        visitBinary(constraint);
    }

    public void visitNotEqual(NotEqualConstraint constraint) {
        constraint.getLeftOperand().visitOperand(this);
        constraint.getRightOperand().visitOperand(this);
    }

    private void visitBinary(BinaryOperandConstraint operandConstraint) {
        operandConstraint.getLeftOperand().visitOperand(this);
        operandConstraint.getRightOperand().visitOperand(this);
    }

    private void visitArray(ArrayConstraint constraint) {
        final Constraint[] constraints = constraint.getConstraints();
        for (Constraint c : constraints) {
            c.accept(this);
        }
    }

    public void visitAnd(AndConstraint constraint) {
        visitArray(constraint);
    }

    public void visitOr(OrConstraint constraint) {
        visitArray(constraint);
    }

    public void visitLessThan(LessThanConstraint constraint) {
        visitBinary(constraint);
    }

    public void visitBiggerThan(BiggerThanConstraint constraint) {
        visitBinary(constraint);
    }

    public void visitStrictlyBiggerThan(StrictlyBiggerThanConstraint constraint) {
        visitBinary(constraint);
    }

    public void visitStrictlyLesserThan(StrictlyLesserThanConstraint constraint) {
        visitBinary(constraint);
    }

    public void visitIn(InConstraint inConstraint) {
        bindInValues(inConstraint.getField(), inConstraint.getValues());
    }

    public void visitIsOrNotNull(NullOrNotConstraint constraint) {
    }

    public void visitNotIn(NotInConstraint constraint) {
        // this used to bind nothing at all, while the WHERE clause was written with one placeholder
        // per value: every notIn constraint reached the database with unbound parameters
        bindInValues(constraint.getField(), constraint.getValues());
    }

    private void bindInValues(Field field, Set<?> values) {
        if (values.isEmpty()) {
            // rendered as a constant predicate, so there is nothing to bind
            return;
        }
        Object last = null;
        int bound = 0;
        for (Object value : values) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("at " + index + " value : " + value);
            }
            setValue(value, ++index);
            field.safeAccept(this);
            last = value;
            bound++;
        }
        // the clause is written with a rounded up number of placeholders: pad by repeating a value
        // already in the set, which changes neither IN nor NOT IN -- padding with NULL would, since
        // "x NOT IN (1, NULL)" is never true
        for (int i = bound; i < InClause.placeholderCount(bound); i++) {
            setValue(last, ++index);
            field.safeAccept(this);
        }
    }

    public void visitContains(Field field, String value, ContainType containType, boolean contains, boolean ignoreCase) {
        final String pattern = switch (containType) {
            case contains -> "%" + value + "%";
            case startWith -> value + "%";
            case endWith -> "%" + value;
        };
        setValue(pattern, ++index);
        field.safeAccept(this);
    }

    @Override
    public void visitRegularExpression(Field field, String value, boolean caseInsensitive, boolean not) {
        setValue(value, ++index);
        field.safeAccept(this);
    }

    public void visitValueOperand(ValueOperand value) {
        Object o = value.getValue();
        if (o == null) {
            throw new UnexpectedApplicationState("null not supported, Should be explicit (is null) for field " + value.getField().getFullName());
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("at " + index + " value : " + o);
        }
        setValue(o, ++index);
        value.getField().safeAccept(this);
    }

    public void visitAccessorOperand(AccessorOperand accessorOperand) {
        Object objectValue = accessorOperand.getAccessor().getObjectValue();
        if (objectValue == null) {
            throw new UnexpectedApplicationState("null not supported, Should be explicit (is null) for field " + accessorOperand.getField().getFullName());
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("at " + index + " value : " + objectValue);
        }
        setValue(objectValue, ++index);
        accessorOperand.getField().safeAccept(this);
    }

    public void visitFieldOperand(Field field) {
    }

    public void visitNot(NotConstraint constraint) {
        constraint.getConstraint().accept(this);
    }

    public void visitBetween(BetweenConstraint constraint) {
        constraint.getMin().visitOperand(this);
        constraint.getMax().visitOperand(this);
    }

    public void visitExists(ExistsConstraint constraint) {
        bindSubQuery(constraint.getSubQuery());
    }

    public void visitInSubQuery(InSubQueryConstraint constraint) {
        bindSubQuery(constraint.getSubQuery());
    }

    private void bindSubQuery(SubQuery subQuery) {
        if (subQuery.where() != null) {
            subQuery.where().accept(this);
        }
    }
}
