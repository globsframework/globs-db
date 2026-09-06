package org.globsframework.sql.drivers.jdbc.request;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.utils.exceptions.UnexpectedApplicationState;
import org.globsframework.sql.SqlRequest;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.exceptions.SqlException;
import org.globsframework.sql.exceptions.SqlExceptions;
import org.globsframework.sql.drivers.jdbc.impl.ValueConstraintVisitor;
import org.globsframework.sql.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sql.utils.StringPrettyWriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;

public class SqlDeleteBuilder implements SqlRequest {
    private final Constraint constraint;
    private final String sqlStatement;
    private final PreparedStatement preparedStatement;
    private final SqlService sqlService;

    public SqlDeleteBuilder(GlobType globType, Constraint constraint, Connection connection,
                            SqlService sqlService) {
        this.constraint = constraint;
        this.sqlService = sqlService;
        StringPrettyWriter prettyWriter = new StringPrettyWriter();
        prettyWriter.append("DELETE ")
                .append(" FROM ");
        HashSet<GlobType> tables = new HashSet<GlobType>();
        tables.add(globType);
        StringPrettyWriter whereWriter = null;
        if (constraint != null) {
            whereWriter = new StringPrettyWriter();
            constraint.accept(new WhereClauseConstraintVisitor(whereWriter, sqlService, tables));
            if (tables.size() != 1) {
                throw new UnexpectedApplicationState("Only one from clause allowed : jointures are not possible : " +
                        whereWriter.toString());
            }
        }
        for (Iterator<GlobType> it = tables.iterator(); it.hasNext(); ) {
            prettyWriter.append(sqlService.getTableName(it.next(), true))
                    .appendIf(", ", it.hasNext());
        }
        if (whereWriter != null) {
            prettyWriter
                    .append(" WHERE ")
                    .append(whereWriter.toString());
        }
        sqlStatement = prettyWriter.toString();
        try {
            preparedStatement = connection.prepareStatement(sqlStatement);
        } catch (SQLException e) {
            throw new UnexpectedApplicationState("For delete request " + sqlStatement, e);
        }

    }

    public int apply() {
        if (constraint != null) {
            constraint.accept(new ValueConstraintVisitor(preparedStatement, sqlService.getNativeValueBinder()));
        }
        long start = System.nanoTime();
        try {
            int count = preparedStatement.executeUpdate();
            sqlService.getListener().onStatement(sqlStatement, System.nanoTime() - start, count, null);
            return count;
        } catch (SQLException e) {
            // used to be an UnexpectedApplicationState: a delete refused by a foreign key is a
            // ForeignKeyViolation, not an application state problem
            SqlException typed = SqlExceptions.typed("For delete request " + sqlStatement, e);
            sqlService.getListener().onStatement(sqlStatement, System.nanoTime() - start, -1, typed);
            throw typed;
        }
    }

    public void close() {
        try {
            preparedStatement.close();
        } catch (SQLException e) {
            throw new UnexpectedApplicationState("In close", e);
        }
    }
}
