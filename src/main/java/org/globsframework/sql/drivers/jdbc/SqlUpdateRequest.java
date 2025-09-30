package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.model.Key;
import org.globsframework.core.utils.NanoChrono;
import org.globsframework.core.utils.exceptions.UnexpectedApplicationState;
import org.globsframework.sql.BatchSqlRequest;
import org.globsframework.sql.SqlRequest;
import org.globsframework.sql.SqlService;
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
    private GlobType globType;
    private Constraint constraint;
    private BlobUpdater blobUpdater;
    private SqlUpdateBuilder.FieldWithAccessor[] values;
    private SqlService sqlService;
    private PreparedStatement preparedStatement;
    private SqlValueFieldVisitor sqlValueFieldVisitor;
    private String sqlRequest;

    public SqlUpdateRequest(GlobType globType, Constraint constraint, SqlUpdateBuilder.FieldWithAccessor[] values,
                            Connection connection, SqlService sqlService, BlobUpdater blobUpdater) {
        this.globType = globType;
        this.constraint = constraint;
        this.blobUpdater = blobUpdater;
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
        sqlValueFieldVisitor = new SqlValueFieldVisitor(preparedStatement, blobUpdater);
    }

    public int apply() {
        updateStatement();
        try {
            NanoChrono nanoChrono = NanoChrono.start();
            final int count = preparedStatement.executeUpdate();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Update request " + sqlRequest + " took " + nanoChrono.getElapsedTimeInMS() + " ms.");
            }
            return count;
        } catch (SQLException e) {
            String message = "For request : " + sqlRequest;
            LOGGER.error(message, e);
            throw new UnexpectedApplicationState(message, e);
        }
    }

    private void updateStatement() {
        for (int i = 0; i < values.length; i++) {
            sqlValueFieldVisitor.setValue(values[i].accessor().getObjectValue(), i + 1);
            values[i].field().safeAccept(sqlValueFieldVisitor);
        }
        constraint.accept(new ValueConstraintVisitor(preparedStatement, values.length, blobUpdater));
    }

    public void close() {
        try {
            preparedStatement.close();
        } catch (SQLException e) {
            throw new UnexpectedApplicationState("In close", e);
        }
    }

    public void execute(Key key) {
        GlobType globType = key.getGlobType();
        Field[] list = globType.getKeyFields();
        Constraint constraint = null;
        for (Field field : list) {
            constraint = Constraints.and(constraint, Constraints.equalsObject(field, key.getValue(field)));
        }
        this.constraint = Constraints.and(this.constraint, constraint);
        apply();
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
