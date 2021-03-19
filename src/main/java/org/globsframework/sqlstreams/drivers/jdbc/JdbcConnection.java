package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.*;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sqlstreams.drivers.jdbc.request.SqlCreateBuilder;
import org.globsframework.sqlstreams.drivers.jdbc.request.SqlDeleteBuilder;
import org.globsframework.sqlstreams.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sqlstreams.drivers.jdbc.request.SqlUpdateBuilder;
import org.globsframework.sqlstreams.exceptions.ConstraintViolation;
import org.globsframework.sqlstreams.exceptions.RollbackFailed;
import org.globsframework.sqlstreams.exceptions.SqlException;
import org.globsframework.sqlstreams.metadata.DbChecker;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;
import org.globsframework.utils.collections.MultiMap;
import org.globsframework.utils.exceptions.GlobsException;
import org.globsframework.utils.exceptions.OperationDenied;
import org.globsframework.utils.exceptions.UnexpectedApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

public abstract class JdbcConnection implements SqlConnection {
    private static Logger LOGGER = LoggerFactory.getLogger(JdbcConnection.class);
    protected JdbcSqlService sqlService;
    private Connection connection;
    private BlobUpdater blobUpdater;
    private DbChecker checker;

    public JdbcConnection(Connection connection, JdbcSqlService sqlService, BlobUpdater blobUpdater) {
        this.connection = connection;
        this.sqlService = sqlService;
        this.blobUpdater = blobUpdater;
        checker = new DbChecker(sqlService, this);
    }

    public SelectBuilder getQueryBuilder(GlobType globType) {
        checkConnectionIsNotClosed();
        return new SqlQueryBuilder(connection, globType, null, sqlService, blobUpdater);
    }

    public SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint) {
        checkConnectionIsNotClosed();
        return new SqlQueryBuilder(connection, globType, constraint, sqlService, blobUpdater);
    }

    public UpdateBuilder getUpdateBuilder(GlobType globType, Constraint constraint) {
        checkConnectionIsNotClosed();
        return new SqlUpdateBuilder(connection, globType, sqlService, constraint, blobUpdater);
    }

    private void checkConnectionIsNotClosed() {
        if (connection == null) {
            throw new UnexpectedApplicationState("connection was closed");
        }
    }

    public void commit() throws RollbackFailed {
        checkConnectionIsNotClosed();
        try {
            connection.commit();
        } catch (SQLException e) {
            throw getTypedException(null, e);
        }
    }

    public void commitAndClose() {
        applyAndClose(new DbFunctor() {
            public void doIt() throws SQLException {
                connection.commit();
            }
        });
    }

    public void rollbackAndClose() {
        applyAndClose(new DbFunctor() {
            public void doIt() throws SQLException {
                connection.rollback();
            }
        });
    }

    public CreateBuilder getCreateBuilder(GlobType globType) {
        return new SqlCreateBuilder(connection, globType, sqlService, blobUpdater, this);
    }

    public void createTable(GlobType globType) {
        if (checker.tableExists(globType)) {
            return;
        }
        LOGGER.info("Create table for " + globType.getName());
        StringPrettyWriter writer = new StringPrettyWriter();
        writer.append("CREATE TABLE ")
                .append(sqlService.getTableName(globType))
                .append(" ( ");
        SqlFieldCreationVisitor creationVisitor = getFieldVisitorCreator(writer);
        int count = 1;
        for (Field field : globType.getFields()) {
            field.safeVisit(creationVisitor.appendComma(count != globType.getFieldCount()));
            count++;
        }
        Field[] keyFields = globType.getKeyFields();
        if (keyFields.length != 0) {
            Field last = keyFields[keyFields.length - 1];
            writer.append(", PRIMARY KEY (");
            for (Field field : keyFields) {
                writer.append(sqlService.getColumnName(field))
                        .appendIf(", ", last != field);
            }
            writer.append(") ");
        }
        writer.append(");");
        try {
            PreparedStatement statement = connection.prepareStatement(writer.toString());
            statement.executeUpdate();
            statement.close();
        } catch (SQLException e) {
            throw new UnexpectedApplicationState("Invalid creation request: " + writer.toString(), e);
        }
    }

    public void addColumn(Field... column) {
        MultiMap<GlobType, Field> fieldsToAdd = new MultiMap<>();
        for (Field field : column) {
            fieldsToAdd.put(field.getGlobType(), field);
        }
        for (Map.Entry<GlobType, List<Field>> entry : fieldsToAdd.entries()) {
            GlobType type = entry.getKey();

            GlobTypeExtractor globTypeExtractor = extractType(sqlService.getTableName(type));

            GlobType tableType = globTypeExtractor.extract();

            StringPrettyWriter writer = new StringPrettyWriter();
            writer.append("ALTER TABLE ")
                    .append(sqlService.getTableName(type))
                    .append(" ADD ");
            SqlFieldCreationVisitor creationVisitor = getFieldVisitorCreator(writer);

            Field[] fieldNotInDb = entry.getValue().stream().filter(f -> !tableType.hasField(sqlService.getColumnName(f)))
                    .toArray(Field[]::new);

            for (int i = 0; i < fieldNotInDb.length; i++) {
                Field field = fieldNotInDb[i];
                LOGGER.info("Add column " + field.getFullName());
                field.safeVisit(creationVisitor);
                if (i + 1 < fieldNotInDb.length) {
                    writer.append(", ");
                }
            }
            writer.append(";");
            try {
                PreparedStatement statement = connection.prepareStatement(writer.toString());
                statement.executeUpdate();
                statement.close();
            } catch (SQLException e) {
                GlobTypeExtractor typeExtractor = extractType(sqlService.getTableName(type));
                GlobType newType = typeExtractor.extract();
                if (entry.getValue().stream().allMatch(f -> newType.hasField(sqlService.getColumnName(f)))) {
                    LOGGER.info("Column already added.");
                    return;
                }
                String message = "fail to add column " + entry.getValue() + " : " + writer.toString();
                LOGGER.error(message);
                throw new UnexpectedApplicationState(message, e);
            }
        }
    }

    public void emptyTable(GlobType globType) {
        StringPrettyWriter writer = new StringPrettyWriter();
        writer.append("DELETE FROM ")
                .append(sqlService.getTableName(globType))
                .append(";");
        try {
            PreparedStatement statament = connection.prepareStatement(writer.toString());
            statament.executeUpdate();
            statament.close();
        } catch (SQLException e) {
            throw new UnexpectedApplicationState("Unable to empty table : " + writer.toString(), e);
        }
    }

    public void showDb() {
    }

    public void populate(GlobList all) {
        for (Glob glob : all) {
            CreateBuilder createBuilder = getCreateBuilder(glob.getType());
            for (Field field : glob.getType().getFields()) {
                createBuilder.setObject(field, glob.getValue(field));
            }
            createBuilder.getRequest().run();
        }
    }

    public SqlService getJdbcSqlService() {
        return sqlService;
    }

    abstract protected SqlFieldCreationVisitor getFieldVisitorCreator(StringPrettyWriter prettyWriter);

    public SqlRequest getDeleteRequest(GlobType globType) {
        return new SqlDeleteBuilder(globType, null, connection, sqlService, blobUpdater);
    }

    public SqlRequest getDeleteRequest(GlobType globType, Constraint constraint) {
        return new SqlDeleteBuilder(globType, constraint, connection, sqlService, blobUpdater);
    }

    public Connection getConnection() {
        return connection;
    }

    public SqlException getTypedException(String sql, SQLException e) {
        if ("23000".equals(e.getSQLState())) {
            if (sql == null) {
                return new ConstraintViolation(e);
            } else {
                return new ConstraintViolation(sql, e);
            }
        }
        return new SqlException(e);
    }

    private void applyAndClose(DbFunctor db) {
        if (connection == null) {
            return;
        }
        GlobsException ex = null;
        try {
            db.doIt();
        } catch (SQLException e) {
            ex = getTypedException(null, e);
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                if (ex == null) {
                    ex = new OperationDenied(e);
                }
            } finally {
                connection = null;
            }
            if (ex != null) {
                throw ex;
            }
        }
    }

    public GlobTypeExtractor extractType(String tableName) {
        return new DefaultGlobTypeExtractor(sqlService, tableName);
    }

    interface DbFunctor {
        void doIt() throws SQLException;
    }
}
