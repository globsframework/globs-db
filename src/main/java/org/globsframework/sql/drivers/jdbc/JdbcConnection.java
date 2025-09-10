package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.annotations.AutoIncrement;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.collections.MultiMap;
import org.globsframework.core.utils.exceptions.GlobsException;
import org.globsframework.core.utils.exceptions.OperationDenied;
import org.globsframework.core.utils.exceptions.UnexpectedApplicationState;
import org.globsframework.sql.*;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sql.drivers.jdbc.request.SqlCreateBuilder;
import org.globsframework.sql.drivers.jdbc.request.SqlDeleteBuilder;
import org.globsframework.sql.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sql.drivers.jdbc.request.SqlUpdateBuilder;
import org.globsframework.sql.exceptions.ConstraintViolation;
import org.globsframework.sql.exceptions.RollbackFailed;
import org.globsframework.sql.exceptions.SqlException;
import org.globsframework.sql.metadata.DbChecker;
import org.globsframework.sql.utils.ExtractType;
import org.globsframework.sql.utils.StringPrettyWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public abstract class JdbcConnection implements SqlConnection {
    private static Logger LOGGER = LoggerFactory.getLogger(JdbcConnection.class);
    private final boolean autoCommit;
    protected SqlService sqlService;
    private Connection connection;
    protected BlobUpdater blobUpdater;
    private DbChecker checker;

    public JdbcConnection(boolean autoCommit, Connection connection, SqlService sqlService, BlobUpdater blobUpdater) {
        this.autoCommit = autoCommit;
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

    protected void checkConnectionIsNotClosed() {
        if (connection == null) {
            throw new UnexpectedApplicationState("connection was closed");
        }
    }

    public void commit() throws RollbackFailed {
        checkConnectionIsNotClosed();
        try {
            if (!autoCommit) {
                connection.commit();
            }
        } catch (SQLException e) {
            throw getTypedException(null, e);
        }
    }

    public void commitAndClose() {
        applyAndClose(() -> {
            if (!autoCommit) {
                connection.commit();
            }
        });
    }

    public void rollbackAndClose() {
        applyAndClose(() -> {
            if (!autoCommit) {
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
                .append(sqlService.getTableName(globType, true))
                .append(" ( ");
        SqlFieldCreationVisitor creationVisitor = getFieldVisitorCreator(writer);
        int count = 1;
        for (Field field : globType.getFields()) {
            field.safeAccept(creationVisitor.appendComma(count != globType.getFieldCount()));
            count++;
        }
        Field[] keyFields = globType.getKeyFields();
        if (keyFields.length != 0) {
            Field last = keyFields[keyFields.length - 1];
            writer.append(", PRIMARY KEY (");
            for (Field field : keyFields) {
                writer.append(sqlService.getColumnName(field, true))
                        .appendIf(", ", last != field);
            }
            writer.append(") ");
        }
        writer.append(")");
        endOfRequest(writer);
        try {
            PreparedStatement statement = connection.prepareStatement(writer.toString());
            statement.executeUpdate();
            statement.close();
            LOGGER.info("sql create request : " + writer.toString());
        } catch (SQLException e) {
            String message = "Invalid creation request: " + writer.toString();
            LOGGER.error(message);
            throw new UnexpectedApplicationState(message, e);
        }
    }

    public void endOfRequest(StringPrettyWriter writer) {
        writer.append(";");
    }

    public void addColumn(Field... column) {
        MultiMap<GlobType, Field> fieldsToAdd = new MultiMap<>();
        for (Field field : column) {
            fieldsToAdd.put(field.getGlobType(), field);
        }
        for (Map.Entry<GlobType, List<Field>> entry : fieldsToAdd.entries()) {
            GlobType type = entry.getKey();

            String tableName = sqlService.getTableName(type, true);
            GlobTypeExtractor globTypeExtractor = extractType(tableName);

            GlobType tableType = globTypeExtractor.extract();

            if (tableType == null) {
                LOGGER.error(tableName + " not found.");
            } else {
                StringPrettyWriter writer = new StringPrettyWriter();
                writer.append("ALTER TABLE ")
                        .append(tableName);
                SqlFieldCreationVisitor creationVisitor = getFieldVisitorCreator(writer);

                Field[] fieldNotInDb = entry.getValue().stream().filter(f -> !tableType.hasField(sqlService.getColumnName(f, true)))
                        .toArray(Field[]::new);

                if (fieldNotInDb.length == 0) {
                    continue;
                }

                for (int i = 0; i < fieldNotInDb.length; i++) {
                    Field field = fieldNotInDb[i];
                    LOGGER.info("Add column " + field.getFullName());
                    writer.append(" ADD ");
                    field.safeAccept(creationVisitor);
                    if (i + 1 < fieldNotInDb.length) {
                        writer.append(", ");
                    }
                }
                endOfRequest(writer);
                try {
                    PreparedStatement statement = connection.prepareStatement(writer.toString());
                    statement.executeUpdate();
                    statement.close();
                } catch (SQLException e) {
                    GlobTypeExtractor typeExtractor = extractType(tableName);
                    GlobType newType = typeExtractor.extract();
                    if (entry.getValue().stream().allMatch(f -> newType.hasField(sqlService.getColumnName(f, true)))) {
                        LOGGER.info("Column already added.");
                        return;
                    }
                    String message = "fail to add column " + entry.getValue() + " : " + writer.toString();
                    LOGGER.error(message);
                    throw new UnexpectedApplicationState(message, e);
                }
            }
        }
    }

    public void emptyTable(GlobType globType) {
        StringPrettyWriter writer = new StringPrettyWriter();
        writer.append("DELETE FROM ")
                .append(sqlService.getTableName(globType, true));
        endOfRequest(writer);

        try {
            PreparedStatement statament = connection.prepareStatement(writer.toString());
            statament.executeUpdate();
            statament.close();
        } catch (SQLException e) {
            String message = "Unable to empty table : " + writer.toString();
            LOGGER.error(message);
            throw new SqlException(message, e);
        }
    }

    public void showDb() {
    }

    public void populate(Collection<Glob> all) {
        for (Glob glob : all) {
            CreateBuilder createBuilder = getCreateBuilder(glob.getType());
            for (Field field : glob.getType().getFields()) {
                if (!field.hasAnnotation(AutoIncrement.KEY) || glob.isSet(field)) {
                    createBuilder.setObject(field, glob.getValue(field));
                }
            }
            try (SqlRequest request = createBuilder.getRequest()) {
                request.run();
            }
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
        if ("23000".equals(e.getSQLState()) || "23505".equals(e.getSQLState())) {
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
        return new DefaultGlobTypeExtractor(sqlService, sqlService.getTableName(tableName, false));
    }

    public GlobType extractFromQuery(String query) {
        try {
            final Connection c = getConnection();
            final Statement statement = c.createStatement();
            statement.execute(query);
            final ResultSetMetaData metaData = statement.getResultSet().getMetaData();
            return ExtractType.createFromMetaData(metaData);
        } catch (SQLException e) {
            throw new RuntimeException(query, e);
        }
    }

    interface DbFunctor {
        void doIt() throws SQLException;
    }
}
