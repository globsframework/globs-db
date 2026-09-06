package org.globsframework.sql.drivers.jdbc.impl;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.SubQuery;
import org.globsframework.sql.TableRef;
import org.globsframework.sql.drivers.jdbc.ColumnQualifier;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.ConstraintVisitor;
import org.globsframework.sql.constraints.OperandVisitor;
import org.globsframework.sql.constraints.impl.*;
import org.globsframework.sql.utils.StringPrettyWriter;

import java.util.Set;

public class WhereClauseConstraintVisitor implements ConstraintVisitor, OperandVisitor {
    protected StringPrettyWriter prettyWriter;
    private SqlService sqlService;
    private Set<GlobType> globTypes;
    private ColumnQualifier columnQualifier;
    /** the occurrence the column being written belongs to, while it is being written */
    private TableRef currentTable;

    public WhereClauseConstraintVisitor(StringPrettyWriter prettyWriter, SqlService sqlService,
                                        Set<GlobType> GlobeTypeSetToUpdate) {
        this.prettyWriter = prettyWriter;
        this.sqlService = sqlService;
        this.globTypes = GlobeTypeSetToUpdate;
        this.columnQualifier = ColumnQualifier.byTableName(sqlService, GlobeTypeSetToUpdate);
    }

    /**
     * Set by a query that joins, so that columns are written with the alias of their occurrence
     * rather than with a table name. Left alone by the delete and update requests, which have a
     * single table and keep the joinless rendering.
     */
    public void setColumnQualifier(ColumnQualifier columnQualifier) {
        this.columnQualifier = columnQualifier;
    }

    private void withTable(TableRef table, Runnable body) {
        TableRef previous = currentTable;
        currentTable = table;
        try {
            body.run();
        } finally {
            currentTable = previous;
        }
    }

    public void visitEqual(EqualConstraint constraint) {
        visitBinary(constraint, " = ");
    }

    public void visitNotEqual(NotEqualConstraint constraint) {
        visitBinary(constraint, " <> ");
    }

    public void visitAnd(AndConstraint constraint) {
        visitArray(constraint, " AND ");
    }

    public void visitOr(OrConstraint constraint) {
        visitArray(constraint, " OR ");
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
        withTable(inConstraint.getTable(),
                () -> appendInClause(inConstraint.getField(), inConstraint.getValues(), false));
    }

    public void visitIsOrNotNull(NullOrNotConstraint constraint) {
        visitFieldOperand(constraint.getField(), constraint.getTable());
        if (constraint.checkNull()) {
            prettyWriter.append(" IS NULL ");
        } else {
            prettyWriter.append(" IS NOT NULL ");
        }
    }

    public void visitNotIn(NotInConstraint constraint) {
        withTable(constraint.getTable(),
                () -> appendInClause(constraint.getField(), constraint.getValues(), true));
    }

    private void appendInClause(Field field, Set<?> values, boolean not) {
        if (values.isEmpty()) {
            // "x IN ()" is not valid SQL anywhere: an empty set matches no row, its complement every
            // row. The table still has to reach the FROM clause.
            globTypes.add(field.getGlobType());
            prettyWriter.append(not ? " 1 = 1 " : " 1 = 0 ");
            return;
        }
        visitFieldOperand(field);
        prettyWriter.append(not ? " NOT IN (" : " in (");
        int count = InClause.placeholderCount(values.size());
        for (int i = 0; i < count; i++) {
            prettyWriter.append(" ? ").appendIf(", ", i < count - 1);
        }
        prettyWriter.append(")");
    }

    public void visitContains(Field field, String value, ContainType containType, boolean contains, boolean ignoreCase) {
        if (ignoreCase) {
            final String likeIgnoreCase = sqlService.getLikeIgnoreCase();
            if (likeIgnoreCase != null) {
                visitFieldOperand(field);
            } else {
                prettyWriter.append(" lower(");
                visitFieldOperand(field);
                prettyWriter.append(") ");
            }
        } else {
            visitFieldOperand(field);
        }
        if (!contains) {
            prettyWriter.append(" NOT ");
        }

        if (ignoreCase) {
            final String likeIgnoreCase = sqlService.getLikeIgnoreCase();
            if (likeIgnoreCase != null) {
                prettyWriter.append(" ").append(likeIgnoreCase).append(" ? ");
            } else {
                prettyWriter.append(" LIKE lower( ? ) ");
            }
        } else {
            prettyWriter.append(" LIKE ? ");
        }
    }

    @Override
    public void visitRegularExpression(Field field, String value, boolean caseInsensitive, boolean not) {
        // default fallback to prevent WHERE
        prettyWriter.append(" 1=1");
    }

    public void visitNot(NotConstraint constraint) {
        prettyWriter.append(" NOT (");
        constraint.getConstraint().accept(this);
        prettyWriter.append(")");
    }

    public void visitBetween(BetweenConstraint constraint) {
        visitFieldOperand(constraint.getField(), constraint.getTable());
        prettyWriter.append(" BETWEEN ");
        constraint.getMin().visitOperand(this);
        prettyWriter.append(" AND ");
        constraint.getMax().visitOperand(this);
    }

    public void visitExists(ExistsConstraint constraint) {
        prettyWriter.append(constraint.isNot() ? " NOT EXISTS (" : " EXISTS (");
        appendSubQuery(constraint.getSubQuery());
        prettyWriter.append(")");
    }

    public void visitInSubQuery(InSubQueryConstraint constraint) {
        visitFieldOperand(constraint.getField(), constraint.getTable());
        prettyWriter.append(constraint.isNot() ? " NOT IN (" : " IN (");
        appendSubQuery(constraint.getSubQuery());
        prettyWriter.append(")");
    }

    private void appendSubQuery(SubQuery subQuery) {
        TableRef table = subQuery.table();
        prettyWriter.append("SELECT ");
        if (subQuery.selected() == null) {
            prettyWriter.append("1");
        } else {
            prettyWriter.append(table.getAlias()).append(".")
                    .append(sqlService.getColumnName(subQuery.selected(), true));
        }
        prettyWriter.append(" FROM ").append(sqlService.getTableName(table.getType(), true))
                .append(" ").append(table.getAlias());
        if (subQuery.where() == null) {
            return;
        }
        prettyWriter.append(" WHERE ");
        // inside the subquery a bare field of its own type means the subquery; anything else is a
        // reference out to the enclosing query, which is what makes it correlated
        ColumnQualifier enclosing = columnQualifier;
        columnQualifier = (field, ref) -> ref != null ? ref.getAlias()
                : field.getGlobType() == table.getType() ? table.getAlias()
                : enclosing.qualify(field, null);
        try {
            subQuery.where().accept(this);
        } finally {
            columnQualifier = enclosing;
        }
    }

    public void visitValueOperand(ValueOperand value) {
        prettyWriter.append(" ? ");
    }

    public void visitAccessorOperand(AccessorOperand accessorOperand) {
        prettyWriter.append(" ? ");
    }

    public void visitFieldOperand(Field field) {
        prettyWriter.append(columnQualifier.qualify(field, currentTable))
                .append(".")
                .append(sqlService.getColumnName(field, true));
    }

    public void visitFieldOperand(Field field, TableRef table) {
        withTable(table, () -> visitFieldOperand(field));
    }

    public void visitContains(Field field, TableRef table, String value, ContainType containType,
                              boolean contains, boolean ignoreCase) {
        // routed through the single argument form, which a dialect may override
        withTable(table, () -> visitContains(field, value, containType, contains, ignoreCase));
    }

    public void visitRegularExpression(Field field, TableRef table, String value, boolean caseInsensitive,
                                       boolean not) {
        withTable(table, () -> visitRegularExpression(field, value, caseInsensitive, not));
    }

    private void visitBinary(BinaryOperandConstraint constraint, String operator) {
        constraint.getLeftOperand().visitOperand(this);
        prettyWriter.append(operator);
        constraint.getRightOperand().visitOperand(this);
    }

    private void visitArray(ArrayConstraint constraint, String operator) {
        prettyWriter.append("(");
        for (Constraint constraintConstraint : constraint.getConstraints()) {
            constraintConstraint.accept(this);
            prettyWriter.append(operator);
        }
        prettyWriter.removeLast(operator.length());
        prettyWriter.append(")").newLine();
    }
}
