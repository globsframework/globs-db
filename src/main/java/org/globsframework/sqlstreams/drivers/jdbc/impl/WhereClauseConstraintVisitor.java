package org.globsframework.sqlstreams.drivers.jdbc.impl;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.constraints.ConstraintVisitor;
import org.globsframework.sqlstreams.constraints.OperandVisitor;
import org.globsframework.sqlstreams.constraints.impl.*;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;

import java.util.Set;

public class WhereClauseConstraintVisitor implements ConstraintVisitor, OperandVisitor {
    private StringPrettyWriter prettyWriter;
    private SqlService sqlService;
    private Set<GlobType> globTypes;

    public WhereClauseConstraintVisitor(StringPrettyWriter prettyWriter, SqlService sqlService,
                                        Set<GlobType> GlobeTypeSetToUpdate) {
        this.prettyWriter = prettyWriter;
        this.sqlService = sqlService;
        this.globTypes = GlobeTypeSetToUpdate;
    }

    public void visitEqual(EqualConstraint constraint) {
        visitBinary(constraint, " = ");
    }

    public void visitNotEqual(NotEqualConstraint constraint) {
        visitBinary(constraint, " <> ");
    }

    public void visitAnd(AndConstraint constraint) {
        visitBinary(constraint, " AND ");
    }

    public void visitOr(OrConstraint constraint) {
        visitBinary(constraint, " OR ");
    }

    public void visitLessThan(LessThanConstraint constraint) {
        visitBinary(constraint, " <= ");
    }

    public void visitBiggerThan(BiggerThanConstraint constraint) {
        visitBinary(constraint, " >= ");
    }

    public void visitStrictlyBiggerThan(StrictlyBiggerThanConstraint constraint) {
        visitBinary(constraint, " > ");
    }

    public void visitStrictlyLesserThan(StrictlyLesserThanConstraint constraint) {
        visitBinary(constraint, " < ");
    }

    public void visitIn(InConstraint inConstraint) {
        visitFieldOperand(inConstraint.getField());
        prettyWriter.append(" in (");
        int length = inConstraint.getValues().size();
        for (int i = 0; i < length; i++) {
            prettyWriter.append(" ? ").appendIf(", ", i < length - 1);
        }
        prettyWriter.append(")");
    }

    public void visitIsOrNotNull(NullOrNotConstraint constraint) {
        visitFieldOperand(constraint.getField());
        if (constraint.checkNull()) {
            prettyWriter.append(" IS NULL ");
        }
        else {
            prettyWriter.append(" IS NOT NULL ");
        }
    }

    public void visitNotIn(NotInConstraint constraint) {
        visitFieldOperand(constraint.getField());
        prettyWriter.append(" NOT IN (");
        int length = constraint.getValues().size();
        for (int i = 0; i < length; i++) {
            prettyWriter.append(" ? ").appendIf(", ", i < length - 1);
        }
        prettyWriter.append(")");
    }

    public void visitContains(Field field, String value, boolean contains, boolean startWith, boolean ignoreCase) {
        if (ignoreCase) {
            final String likeIgnoreCase = sqlService.getLikeIgnoreCase();
            if (likeIgnoreCase != null) {
                visitFieldOperand(field);
            }
            else {
                prettyWriter.append(" lower(");
                visitFieldOperand(field);
                prettyWriter.append(") ");
            }
        }
        else {
            visitFieldOperand(field);
        }
        if (!contains) {
            prettyWriter.append(" NOT ");
        }

//        prettyWriter.append(" LIKE '" + (startWith ? "" : "%") + value + "%'");
        if (ignoreCase) {
            final String likeIgnoreCase = sqlService.getLikeIgnoreCase();
            if (likeIgnoreCase != null) {
                prettyWriter.append(" ").append(likeIgnoreCase).append(" ? ");
            }
            else {
                prettyWriter.append(" LIKE lower( ? ) ");
            }
        }else {
            prettyWriter.append(" LIKE ? " );
        }
    }

    public void visitValueOperand(ValueOperand value) {
        prettyWriter.append(" ? ");
    }

    public void visitAccessorOperand(AccessorOperand accessorOperand) {
        prettyWriter.append(" ? ");
    }

    public void visitFieldOperand(Field field) {
        globTypes.add(field.getGlobType());
        prettyWriter.append(sqlService.getTableName(field.getGlobType()))
                .append(".")
                .append(sqlService.getColumnName(field));
    }

    private void visitBinary(BinaryOperandConstraint constraint, String operator) {
        constraint.getLeftOperand().visitOperand(this);
        prettyWriter.append(operator);
        constraint.getRightOperand().visitOperand(this);
    }

    private void visitBinary(BinaryConstraint constraint, String operator) {
        prettyWriter.append("(");
        constraint.getLeftConstraint().visit(this);
        prettyWriter.append(operator);
        constraint.getRightConstraint().visit(this);
        prettyWriter.append(")").newLine();
    }
}
