package org.globsframework.sql.drivers.jdbc.request;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.utils.exceptions.UnexpectedApplicationState;
import org.globsframework.sql.SqlRequest;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.BlobUpdater;
import org.globsframework.sql.drivers.jdbc.impl.ValueConstraintVisitor;
import org.globsframework.sql.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sql.utils.StringPrettyWriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;

public class SqlDeleteBuilder implements SqlRequest {
    private Constraint constraint;
    private BlobUpdater blobUpdater;
    private String sqlStatement;
    private PreparedStatement preparedStatement;

    public SqlDeleteBuilder(GlobType globType, Constraint constraint, Connection connection,
                            SqlService sqlService, BlobUpdater blobUpdater) {
        this.constraint = constraint;
        this.blobUpdater = blobUpdater;
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
            constraint.accept(new ValueConstraintVisitor(preparedStatement, blobUpdater));
        }
        try {
            return preparedStatement.executeUpdate();
        } catch (SQLException e) {
            throw new UnexpectedApplicationState("For delete request " + sqlStatement, e);
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
