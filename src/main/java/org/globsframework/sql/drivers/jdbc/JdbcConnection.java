package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.annotations.AutoIncrement;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.metamodel.index.*;
import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.collections.MultiMap;
import org.globsframework.core.utils.exceptions.GlobsException;
import org.globsframework.core.utils.exceptions.OperationDenied;
import org.globsframework.core.streams.accessors.utils.ValueAccessor;
import org.globsframework.core.utils.exceptions.UnexpectedApplicationState;
import org.globsframework.sql.*;
import org.globsframework.sql.annotations.DbIndex;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sql.drivers.jdbc.request.SqlCreateBuilder;
import org.globsframework.sql.drivers.jdbc.request.SqlDeleteBuilder;
import org.globsframework.sql.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sql.drivers.jdbc.request.SqlUpdateBuilder;
import org.globsframework.sql.exceptions.ConstraintViolation;
import org.globsframework.sql.exceptions.SqlExceptions;
import org.globsframework.sql.exceptions.RollbackFailed;
import org.globsframework.sql.exceptions.SqlException;
import org.globsframework.sql.metadata.DbChecker;
import org.globsframework.sql.utils.ExtractType;
import org.globsframework.sql.utils.StringPrettyWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class JdbcConnection implements SqlConnection {
    private static Logger LOGGER = LoggerFactory.getLogger(JdbcConnection.class);
    private static final int BATCH_SIZE = 1000;
    private final boolean autoCommit;
    protected SqlService sqlService;
    private Connection connection;
    private DbChecker checker;

    public JdbcConnection(boolean autoCommit, Connection connection, SqlService sqlService) {
        this.autoCommit = autoCommit;
        this.connection = connection;
        this.sqlService = sqlService;
        checker = new DbChecker(sqlService, this);
    }

    public SelectBuilder getQueryBuilder(GlobType globType) {
        checkConnectionIsNotClosed();
        return new SqlQueryBuilder(connection, globType, null, sqlService);
    }

    public SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint) {
        checkConnectionIsNotClosed();
        return new SqlQueryBuilder(connection, globType, constraint, sqlService);
    }

    public UpdateBuilder getUpdateBuilder(GlobType globType, Constraint constraint) {
        checkConnectionIsNotClosed();
        return new SqlUpdateBuilder(connection, globType, sqlService, constraint);
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

    @Override
    public void rollback() throws RollbackFailed, ConstraintViolation {
        checkConnectionIsNotClosed();
        try {
            if (!autoCommit) {
                connection.rollback();
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

    /**
     * Idempotent release, for try-with-resources. Any uncommitted work is rolled back; a failing
     * rollback is logged and swallowed so that it never masks the exception that is unwinding the
     * try block. Use commitAndClose() when the outcome of the commit matters.
     */
    public void close() {
        if (connection == null) {
            return;
        }
        try {
            if (!autoCommit) {
                connection.rollback();
            }
        } catch (SQLException e) {
            LOGGER.warn("Rollback failed on close", e);
        } finally {
            try {
                connection.close();
            } catch (SQLException e) {
                LOGGER.warn("Failed to close connection", e);
            } finally {
                connection = null;
            }
        }
    }

    public CreateBuilder getCreateBuilder(GlobType globType) {
        return new SqlCreateBuilder(connection, globType, sqlService, this);
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
        createIndexes(globType);
    }

    public void createIndexes(GlobType globType) {
        checkConnectionIsNotClosed();
        List<Index> indexes = declaredIndexes(globType);
        if (indexes.isEmpty()) {
            return;
        }
        Set<String> known = existingIndexNames(globType);
        for (Index index : indexes) {
            String name = indexName(globType, index);
            if (!known.add(sqlService.getColumnName(name, false))) {
                continue;
            }
            StringPrettyWriter writer = new StringPrettyWriter();
            writer.append("CREATE ");
            writer.appendIf("UNIQUE ", isUnique(index));
            writer.append("INDEX ")
                    .append(sqlService.getColumnName(name, true))
                    .append(" ON ")
                    .append(sqlService.getTableName(globType, true))
                    .append(" (");
            List<Field> fields = index.fields().toList();
            for (int i = 0; i < fields.size(); i++) {
                writer.append(sqlService.getColumnName(fields.get(i), true))
                        .appendIf(", ", i + 1 < fields.size());
            }
            writer.append(")");
            endOfRequest(writer);
            try (PreparedStatement statement = connection.prepareStatement(writer.toString())) {
                statement.executeUpdate();
                LOGGER.info("sql create index request : " + writer);
            } catch (SQLException e) {
                String message = "Invalid index creation request: " + writer;
                LOGGER.error(message);
                throw new UnexpectedApplicationState(message, e);
            }
        }
    }

    /**
     * Indexes declared on the type itself, plus those carried by a DbIndex annotation.
     */
    private static List<Index> declaredIndexes(GlobType globType) {
        List<Index> indexes = new ArrayList<>(globType.getIndices());
        globType.streamAnnotations(DbIndex.TYPE)
                .map(annotation -> DbIndex.createIndex(globType, annotation))
                .forEach(indexes::add);
        return indexes;
    }

    /**
     * An index name is unique per schema on PostgreSQL and HSQLDB, not per table, so the declared
     * name is qualified by the table it belongs to.
     */
    private String indexName(GlobType globType, Index index) {
        return sqlService.getTableName(globType, false) + "_" + index.getName();
    }

    private Set<String> existingIndexNames(GlobType globType) {
        Set<String> names = new HashSet<>();
        try (ResultSet indexInfo = connection.getMetaData().getIndexInfo(connection.getCatalog(), null,
                sqlService.getTableName(globType, false), false, true)) {
            while (indexInfo.next()) {
                String name = indexInfo.getString("INDEX_NAME");
                if (name != null) {
                    names.add(name);
                }
            }
        } catch (SQLException e) {
            // not fatal: at worst an index we already have is created again and the DDL fails loudly
            LOGGER.warn("Could not list the indexes of " + globType.getName(), e);
        }
        return names;
    }

    private static boolean isUnique(Index index) {
        boolean[] unique = new boolean[1];
        index.visit(new IndexVisitor() {
            public void visitUniqueIndex(UniqueIndex index) {
                unique[0] = true;
            }

            public void visitNotUniqueIndex(NotUniqueIndex index) {
            }

            public void visitNotUnique(MultiFieldNotUniqueIndex index) {
            }

            public void visitUnique(MultiFieldUniqueIndex index) {
                unique[0] = true;
            }
        });
        return unique[0];
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
            // extractType maps the name itself, so it takes the unescaped one: handing it an escaped
            // name means looking up a table whose name contains the quotes
            GlobTypeExtractor globTypeExtractor = extractType(sqlService.getTableName(type, false));

            GlobType tableType = globTypeExtractor.extract();

            if (tableType == null) {
                LOGGER.error(sqlService.getTableName(type, false) + " not found.");
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
                    GlobTypeExtractor typeExtractor = extractType(sqlService.getTableName(type, false));
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

    /**
     * Inserts every glob, one prepared statement per shape rather than one per row — a shape being a
     * type together with the columns actually written, which an unset auto-increment key makes vary
     * inside a single type. Batches are flushed every {@value BATCH_SIZE} rows so a large collection
     * does not pile up in the driver.
     */
    public void populate(Collection<Glob> all) {
        checkConnectionIsNotClosed();
        Map<Shape, BatchPopulate> batches = new LinkedHashMap<>();
        try {
            for (Glob glob : all) {
                Shape shape = new Shape(glob.getType(), columnsToWrite(glob));
                batches.computeIfAbsent(shape, s -> new BatchPopulate(getCreateBuilder(s.type()), s.columns()))
                        .add(glob);
            }
            for (BatchPopulate batch : batches.values()) {
                batch.flush();
            }
        } finally {
            for (BatchPopulate batch : batches.values()) {
                batch.close();
            }
        }
    }

    private static List<Field> columnsToWrite(Glob glob) {
        List<Field> columns = new ArrayList<>();
        for (Field field : glob.getType().getFields()) {
            if (!field.hasAnnotation(AutoIncrement.KEY) || glob.isSet(field)) {
                columns.add(field);
            }
        }
        return columns;
    }

    private record Shape(GlobType type, List<Field> columns) {
    }

    private static class BatchPopulate {
        private final List<Field> columns;
        private final ValueAccessor[] accessors;
        private final BatchSqlRequest request;
        private int pending;

        BatchPopulate(CreateBuilder createBuilder, List<Field> columns) {
            this.columns = columns;
            this.accessors = new ValueAccessor[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                accessors[i] = new ValueAccessor();
                createBuilder.setObject(columns.get(i), accessors[i]);
            }
            this.request = createBuilder.getBulkRequest();
        }

        void add(Glob glob) {
            for (int i = 0; i < columns.size(); i++) {
                accessors[i].setValue(glob.getValue(columns.get(i)));
            }
            request.addBatch();
            if (++pending == BATCH_SIZE) {
                flush();
            }
        }

        void flush() {
            if (pending != 0) {
                request.applyBatch();
                pending = 0;
            }
        }

        void close() {
            request.close();
        }
    }

    public SqlService getJdbcSqlService() {
        return sqlService;
    }

    abstract protected SqlFieldCreationVisitor getFieldVisitorCreator(StringPrettyWriter prettyWriter);

    public SqlRequest getDeleteRequest(GlobType globType) {
        return new SqlDeleteBuilder(globType, null, connection, sqlService);
    }

    public SqlRequest getDeleteRequest(GlobType globType, Constraint constraint) {
        return new SqlDeleteBuilder(globType, constraint, connection, sqlService);
    }

    public Connection getConnection() {
        return connection;
    }

    /**
     * Classifies a driver exception. Only 23000 and 23505 used to be recognised, and everything else
     * became an opaque SqlException — which left a caller no way to tell a duplicate key from a
     * deadlock, and so no way to decide whether retrying made sense. See {@link SqlExceptions}.
     */
    public SqlException getTypedException(String sql, SQLException e) {
        return SqlExceptions.typed(sql, e);
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
        return new DefaultGlobTypeExtractor(sqlService, this, sqlService.getTableName(tableName, false));
    }

    public GlobType extractFromQuery(String query) {
        // the statement must be closed: on a pooled connection a dangling cursor outlives the call
        try (Statement statement = getConnection().createStatement()) {
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
