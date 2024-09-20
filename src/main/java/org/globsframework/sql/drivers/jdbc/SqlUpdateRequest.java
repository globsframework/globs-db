package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.model.Key;
import org.globsframework.core.streams.accessors.Accessor;
import org.globsframework.core.utils.NanoChrono;
import org.globsframework.core.utils.exceptions.UnexpectedApplicationState;
import org.globsframework.sql.SqlRequest;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.drivers.jdbc.impl.SqlValueFieldVisitor;
import org.globsframework.sql.drivers.jdbc.impl.ValueConstraintVisitor;
import org.globsframework.sql.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sql.utils.StringPrettyWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.*;

public class SqlUpdateRequest implements SqlRequest {
    private final static Logger LOGGER = LoggerFactory.getLogger(SqlUpdateRequest.class);
    private GlobType globType;
    private Constraint constraint;
    private BlobUpdater blobUpdater;
    private Map<Field, Accessor> values;
    private SqlService sqlService;
    private PreparedStatement preparedStatement;
    private SqlValueFieldVisitor sqlValueFieldVisitor;
    private String sqlRequest;

    public SqlUpdateRequest(GlobType globType, Constraint constraint, Map<Field, Accessor> values,
                            Connection connection, SqlService sqlService, BlobUpdater blobUpdater) {
        this.globType = globType;
        this.constraint = constraint;
        this.blobUpdater = blobUpdater;
        this.values = new HashMap<Field, Accessor>(values);
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

    public int run() {
        int index = 0;
        for (Map.Entry<Field, Accessor> entry : values.entrySet()) {
            sqlValueFieldVisitor.setValue(entry.getValue().getObjectValue(), ++index);
            entry.getKey().safeAccept(sqlValueFieldVisitor);
        }
        constraint.accept(new ValueConstraintVisitor(preparedStatement, index, blobUpdater));
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
        run();
    }

    private String createRequest() {
        StringPrettyWriter prettyWriter = new StringPrettyWriter();
        prettyWriter.append("UPDATE ")
                .append(sqlService.getTableName(globType, true))
                .append(" SET ");
        for (Iterator it = values.keySet().iterator(); it.hasNext(); ) {
            Field field = (Field) it.next();
            prettyWriter
                    .append(sqlService.getColumnName(field, true))
                    .append(" = ?").
                    appendIf(" , ", it.hasNext());
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
}
