package org.globsframework.sqlstreams.drivers.jdbc.impl;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.ConstraintVisitor;
import org.globsframework.sqlstreams.constraints.OperandVisitor;
import org.globsframework.sqlstreams.constraints.impl.*;
import org.globsframework.sqlstreams.drivers.jdbc.BlobUpdater;
import org.globsframework.utils.exceptions.UnexpectedApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;

public class ValueConstraintVisitor extends SqlValueFieldVisitor implements ConstraintVisitor, OperandVisitor {
    private static final Logger LOGGER = LoggerFactory.getLogger(ValueConstraintVisitor.class);
    private int index = 0;

    public ValueConstraintVisitor(PreparedStatement preparedStatement, BlobUpdater blobUpdater) {
        super(preparedStatement, blobUpdater);
    }

    public ValueConstraintVisitor(PreparedStatement preparedStatement, int index, BlobUpdater blobUpdater) {
        super(preparedStatement, blobUpdater);
        this.index = index;
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
            c.visit(this);
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
        Field field = inConstraint.getField();
        for (Object value : inConstraint.getValues()) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("at " + index + " value : " + value);
            }
            setValue(value, ++index);
            field.safeAccept(this);
        }
    }

    public void visitIsOrNotNull(NullOrNotConstraint constraint) {
    }

    public void visitNotIn(NotInConstraint constraint) {

    }

    public void visitContains(Field field, String value, boolean contains, boolean startWith, boolean ignoreCase) {
        if (contains) {
            setValue("%" + value + "%", ++index);
        } else if (startWith) {
            setValue(value + "%", ++index);
        }
        else {
            setValue(value, ++index);
        }
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
}
