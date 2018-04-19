package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.model.Filters;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.globsframework.metamodel.Field;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.annotations.DbRef;
import org.globsframework.sqlstreams.constraints.ConstraintVisitor;
import org.globsframework.sqlstreams.constraints.OperandVisitor;
import org.globsframework.sqlstreams.constraints.impl.*;

import java.util.ArrayList;
import java.util.List;

import static com.mongodb.client.model.Filters.and;
import static com.mongodb.client.model.Filters.or;

class MongoConstraintVisitor implements ConstraintVisitor {
    private SqlService sqlService;
    public Bson filter;

    public MongoConstraintVisitor(SqlService sqlService) {
        this.sqlService = sqlService;
    }

    public void visitEqual(EqualConstraint constraint) {
        ExtractOperandVisitor leftOp = new ExtractOperandVisitor();
        constraint.getLeftOperand().visitOperand(leftOp);
        ExtractOperandVisitor rightOp = new ExtractOperandVisitor();
        constraint.getRightOperand().visitOperand(rightOp);

        if (leftOp.field != null && rightOp.field == null) {
            Object value = adaptData(leftOp.field, rightOp.value);
            filter = Filters.eq(sqlService.getColumnName(leftOp.field), value);
        } else if (rightOp.field != null && leftOp.field == null) {
            Object value = adaptData(rightOp.field, leftOp.value);
            filter = Filters.eq(sqlService.getColumnName(rightOp.field), value);
        } else {
            throw new RuntimeException("Can only call equal between field and value");
        }
    }

    private Object adaptData(Field field, Object value) {
        if (field.hasAnnotation(DbRef.KEY) || (field.isKeyField() && field.getGlobType().getKeyFields().length == 1)) {
            Object value1 = value;
            if (value1 instanceof String) {
                value = new ObjectId((String) value1);
            } else {
                return value;
            }
        }
        return value;
    }

    public void visitNotEqual(NotEqualConstraint constraint) {
        ExtractOperandVisitor leftOp = new ExtractOperandVisitor();
        constraint.getLeftOperand().visitOperand(leftOp);
        ExtractOperandVisitor rightOp = new ExtractOperandVisitor();
        constraint.getRightOperand().visitOperand(rightOp);

        if (leftOp.field != null && rightOp.field == null) {
            filter = Filters.ne(sqlService.getColumnName(leftOp.field), adaptData(leftOp.field, rightOp.value));
        } else if (rightOp.field != null && leftOp.field == null) {
            filter = Filters.ne(sqlService.getColumnName(rightOp.field), adaptData(rightOp.field, leftOp.value));
        } else {
            throw new RuntimeException("Can only call equal between field and value");
        }
    }

    public void visitAnd(AndConstraint constraint) {
        MongoConstraintVisitor left = new MongoConstraintVisitor(sqlService);
        constraint.getLeftConstraint().visit(left);

        MongoConstraintVisitor right = new MongoConstraintVisitor(sqlService);
        constraint.getRightConstraint().visit(right);

        filter = and(left.filter, right.filter);
    }

    public void visitOr(OrConstraint constraint) {
        MongoConstraintVisitor left = new MongoConstraintVisitor(sqlService);
        constraint.getLeftConstraint().visit(left);

        MongoConstraintVisitor right = new MongoConstraintVisitor(sqlService);
        constraint.getRightConstraint().visit(right);

        filter = or(left.filter, right.filter);
    }

    public void visitLessThan(LessThanConstraint constraint) {
        ExtractOperandVisitor leftOp = new ExtractOperandVisitor();
        constraint.getLeftOperand().visitOperand(leftOp);
        ExtractOperandVisitor rightOp = new ExtractOperandVisitor();
        constraint.getRightOperand().visitOperand(rightOp);

        if (leftOp.field != null && rightOp.field == null) {
            filter = Filters.lte(sqlService.getColumnName(leftOp.field), adaptData(leftOp.field, rightOp.value));
        } else if (rightOp.field != null && leftOp.field == null) {
            filter = Filters.gt(sqlService.getColumnName(rightOp.field), adaptData(rightOp.field, leftOp.value));
        } else {
            throw new RuntimeException("Can only call equal between field and value");
        }
    }

    public void visitBiggerThan(BiggerThanConstraint constraint) {
        ExtractOperandVisitor leftOp = new ExtractOperandVisitor();
        constraint.getLeftOperand().visitOperand(leftOp);
        ExtractOperandVisitor rightOp = new ExtractOperandVisitor();
        constraint.getRightOperand().visitOperand(rightOp);

        if (leftOp.field != null && rightOp.field == null) {
            filter = Filters.gte(sqlService.getColumnName(leftOp.field), adaptData(leftOp.field, rightOp.value));
        } else if (rightOp.field != null && leftOp.field == null) {
            filter = Filters.lt(sqlService.getColumnName(rightOp.field), adaptData(rightOp.field, leftOp.value));
        } else {
            throw new RuntimeException("Can only call equal between field and value");
        }
    }

    public void visitStrictlyBiggerThan(StrictlyBiggerThanConstraint constraint) {
        ExtractOperandVisitor leftOp = new ExtractOperandVisitor();
        constraint.getLeftOperand().visitOperand(leftOp);
        ExtractOperandVisitor rightOp = new ExtractOperandVisitor();
        constraint.getRightOperand().visitOperand(rightOp);

        if (leftOp.field != null && rightOp.field == null) {
            filter = Filters.gt(sqlService.getColumnName(leftOp.field), adaptData(leftOp.field, rightOp.value));
        } else if (rightOp.field != null && leftOp.field == null) {
            filter = Filters.lte(sqlService.getColumnName(rightOp.field), adaptData(rightOp.field, leftOp.value));
        } else {
            throw new RuntimeException("Can only call equal between field and value");
        }
    }

    public void visitStrictlyLesserThan(StrictlyLesserThanConstraint constraint) {
        ExtractOperandVisitor leftOp = new ExtractOperandVisitor();
        constraint.getLeftOperand().visitOperand(leftOp);
        ExtractOperandVisitor rightOp = new ExtractOperandVisitor();
        constraint.getRightOperand().visitOperand(rightOp);

        if (leftOp.field != null && rightOp.field == null) {
            filter = Filters.lt(sqlService.getColumnName(leftOp.field), adaptData(leftOp.field, rightOp.value));
        } else if (rightOp.field != null && leftOp.field == null) {
            filter = Filters.gte(sqlService.getColumnName(rightOp.field), adaptData(rightOp.field, leftOp.value));
        } else {
            throw new RuntimeException("Can only call equal between field and value");
        }
    }

    public void visitIn(InConstraint constraint) {
        String fieldName = sqlService.getColumnName(constraint.getField());
        List<Object> converted = new ArrayList<>();
        for (Object o : constraint.getValues()) {
            converted.add(adaptData(constraint.getField(), o));
        }
        filter = Filters.in(fieldName, converted);
    }

    public void visitIsOrNotNull(NullOrNotConstraint constraint) {
        String fieldName = sqlService.getColumnName(constraint.getField());
        if (constraint.checkNull()) {
            filter = Filters.not(Filters.exists(fieldName));
        } else {
            filter = Filters.exists(fieldName);
        }
    }

    public void visitNotIn(NotInConstraint constraint) {
        String fieldName = sqlService.getColumnName(constraint.getField());
        List<Object> converted = new ArrayList<>();
        for (Object o : constraint.getValues()) {
            converted.add(adaptData(constraint.getField(), o));
        }
        filter = Filters.nin(fieldName, converted);

    }

    public void visitContains(Field field, String value, boolean contains) {
        Bson regex = Filters.regex(sqlService.getColumnName(field), ".*" + value + ".*");
        if (contains) {
            filter = regex;
        } else {
            filter = Filters.not(regex);
        }
    }

    private static class ExtractOperandVisitor implements OperandVisitor {
        private Object value;
        private Field field;

        public void visitValueOperand(ValueOperand value) {
            this.value = value.getValue();
        }

        public void visitAccessorOperand(AccessorOperand accessorOperand) {
            value = accessorOperand.getAccessor().getObjectValue();
        }

        public void visitFieldOperand(Field field) {
            this.field = field;
        }
    }
}
