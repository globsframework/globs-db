package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.model.Key;
import org.globsframework.core.utils.NanoChrono;
import org.globsframework.core.utils.exceptions.UnexpectedApplicationState;
import org.globsframework.sql.BatchSqlRequest;
import org.globsframework.sql.SqlRequest;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.exceptions.SqlException;
import org.globsframework.sql.exceptions.SqlExceptions;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.drivers.jdbc.impl.SqlValueFieldVisitor;
import org.globsframework.sql.drivers.jdbc.impl.ValueConstraintVisitor;
import org.globsframework.sql.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sql.drivers.jdbc.request.SqlUpdateBuilder;
import org.globsframework.sql.exceptions.SqlException;
import org.globsframework.sql.utils.StringPrettyWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class SqlUpdateRequest implements SqlRequest, BatchSqlRequest {
    private final static Logger LOGGER = LoggerFactory.getLogger(SqlUpdateRequest.class);
    private final GlobType globType;
    private final Constraint constraint;
    private final SqlUpdateBuilder.FieldWithAccessor[] values;
    private final SqlService sqlService;
    private final PreparedStatement preparedStatement;
    private final SqlValueFieldVisitor sqlValueFieldVisitor;
    private final String sqlRequest;

    public SqlUpdateRequest(GlobType globType, Constraint constraint, SqlUpdateBuilder.FieldWithAccessor[] values,
                            Connection connection, SqlService sqlService) {
        this.globType = globType;
        this.constraint = constraint;
        this.values = values;
        this.sqlService = sqlService;
        sqlRequest = createRequest();
        try {
            NanoChrono nanoChrono = NanoChrono.start();
            preparedStatement = connection.prepareStatement(sqlRequest);
            LOGGER.info("Update request " + sqlRequest + " took " + nanoChrono.getElapsedTimeInMS() + " ms.");
        } catch (SQLException e) {
            String message = "For request : " + sqlRequest;
            LOGGER.error(message, e);
            throw new UnexpectedApplicationState(message, e);
        }
        sqlValueFieldVisitor = new SqlValueFieldVisitor(preparedStatement, sqlService.getNativeValueBinder());
    }

    public int apply() {
        updateStatement();
        long start = System.nanoTime();
        try {
            final int count = preparedStatement.executeUpdate();
            sqlService.getListener().onStatement(sqlRequest, System.nanoTime() - start, count, null);
            return count;
        } catch (SQLException e) {
            String message = "For request : " + sqlRequest;
            LOGGER.error(message, e);
            // used to be an UnexpectedApplicationState, which hid a constraint violation behind a
            // type nobody catches
            SqlException typed = SqlExceptions.typed(message, e);
            sqlService.getListener().onStatement(sqlRequest, System.nanoTime() - start, -1, typed);
            throw typed;
        }
    }

    private void updateStatement() {
        for (int i = 0; i < values.length; i++) {
            sqlValueFieldVisitor.setValue(values[i].accessor().getObjectValue(), i + 1);
            values[i].field().safeAccept(sqlValueFieldVisitor);
        }
        constraint.accept(new ValueConstraintVisitor(preparedStatement, values.length,
                sqlService.getNativeValueBinder()));
    }

    public void close() {
        try {
            preparedStatement.close();
        } catch (SQLException e) {
            throw new UnexpectedApplicationState("In close", e);
        }
    }

    private String createRequest() {
        StringPrettyWriter prettyWriter = new StringPrettyWriter();
        prettyWriter.append("UPDATE ")
                .append(sqlService.getTableName(globType, true))
                .append(" SET ");
        for (int i = 0; i < values.length; i++) {
            SqlUpdateBuilder.FieldWithAccessor value = values[i];
            Field field = value.field();
            prettyWriter
                    .append(sqlService.getColumnName(field, true))
                    .append(" = ?").
                    appendIf(" , ", i != values.length - 1);
        }
        prettyWriter.append(" WHERE ");
        Set<GlobType> globTypes = new HashSet<GlobType>();
        globTypes.add(globType);
        constraint.accept(new WhereClauseConstraintVisitor(prettyWriter, sqlService, globTypes));
        if (globTypes.size() > 1) {
            throw new UnexpectedApplicationState("Only the updated table is valide in query " + prettyWriter.toString());
        }

        return prettyWriter.toString();
    }

    @Override
    public void addBatch() throws SqlException {
        updateStatement();
        try {
            preparedStatement.addBatch();
        } catch (SQLException e) {
            String message = "For request : " + sqlRequest;
            LOGGER.error(message, e);
            throw new UnexpectedApplicationState(message, e);
        }

    }

    @Override
    public int[] applyBatch() {
        try {
            long start = System.nanoTime();
            final int[] results = preparedStatement.executeBatch();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Apply batch request " + sqlRequest + " in " + TimeUnit.NANOSECONDS.toMicros(start - System.nanoTime()) + " us.");
            }
            return results;
        } catch (SQLException e) {
            String message = "For request : " + sqlRequest;
            LOGGER.error(message, e);
            throw new UnexpectedApplicationState(message, e);
        }
    }
}
