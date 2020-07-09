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
import org.globsframework.utils.exceptions.GlobsException;
import org.globsframework.utils.exceptions.OperationDenied;
import org.globsframework.utils.exceptions.UnexpectedApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;

public abstract class JdbcConnection implements SqlConnection {
    private static Logger LOGGER = LoggerFactory.getLogger(JdbcConnection.class);

    private Connection connection;
    protected JdbcSqlService sqlService;
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

    interface DbFunctor {
        void doIt() throws SQLException;
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

    public void addColumn(Field column) {
        GlobTypeExtractor globTypeExtractor = extractType(sqlService.getTableName(column.getGlobType()));
        if (globTypeExtractor.extract().hasField(sqlService.getColumnName(column))) {
            return;
        }
        LOGGER.info("Add column " + column.getFullName());
        StringPrettyWriter writer = new StringPrettyWriter();
        writer.append("ALTER TABLE ")
                .append(sqlService.getTableName(column.getGlobType()))
        .append(" ADD ");
        SqlFieldCreationVisitor creationVisitor = getFieldVisitorCreator(writer);
        column.safeVisit(creationVisitor);
        writer.append(";");
        try {
            PreparedStatement statament = connection.prepareStatement(writer.toString());
            statament.executeUpdate();
            statament.close();
        } catch (SQLException e) {
            GlobTypeExtractor typeExtractor = extractType(sqlService.getTableName(column.getGlobType()));
            if (typeExtractor.extract().hasField(sqlService.getColumnName(column))) {
                LOGGER.info("Column already added.");
                return;
            }
            throw new UnexpectedApplicationState("add column " + column.getFullName() + " : " + writer.toString(), e);
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
}
