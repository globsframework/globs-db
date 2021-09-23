package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.accessors.SqlAccessor;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.jdbc.impl.ValueConstraintVisitor;
import org.globsframework.sqlstreams.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sqlstreams.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;
import org.globsframework.streams.DbStream;
import org.globsframework.utils.exceptions.ItemNotFound;
import org.globsframework.utils.exceptions.TooManyItems;
import org.globsframework.utils.exceptions.UnexpectedApplicationState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SqlSelectQuery implements SelectQuery {
    private static Logger LOGGER = LoggerFactory.getLogger(SqlSelectQuery.class);
    private Set<GlobType> globTypes = new HashSet<GlobType>();
    private Constraint constraint;
    private BlobUpdater blobUpdater;
    private boolean autoClose;
    private Map<Field, SqlAccessor> fieldToAccessorHolder;
    private SqlService sqlService;
    private final List<SqlQueryBuilder.Order> orders;
    private final int top;
    private Set<Field> distinct;
    private List<SqlOperation> sqlOperations;
    private PreparedStatement preparedStatement;
    private String sql;
    private boolean shouldInitAccessorWithMetadata;

    public SqlSelectQuery(Connection connection, Constraint constraint,
                          Map<Field, SqlAccessor> fieldToAccessorHolder, SqlService sqlService,
                          BlobUpdater blobUpdater, boolean autoClose, List<SqlQueryBuilder.Order> orders,
                          int top, Set<Field> distinct, List<SqlOperation> sqlOperations) {
        this.constraint = constraint;
        this.blobUpdater = blobUpdater;
        this.autoClose = autoClose;
        this.fieldToAccessorHolder = new HashMap<>(fieldToAccessorHolder);
        this.sqlService = sqlService;
        this.orders = orders;
        this.top = top;
        this.distinct = distinct;
        this.sqlOperations = sqlOperations;
        String externalRequest = null;
        if (externalRequest == null) {
            sql = prepareSqlRequest();
        } else {
            sql = externalRequest;
        }
        try {
            this.preparedStatement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            // ack => force mysql driver to not load all data
            if (preparedStatement instanceof com.mysql.jdbc.PreparedStatement) {
                ((com.mysql.jdbc.PreparedStatement) preparedStatement).enableStreamingResults();
            }
        } catch (SQLException e) {
            throw new UnexpectedApplicationState("for request " + sql, e);
        }
        shouldInitAccessorWithMetadata = externalRequest != null;
    }

    private void initIndexFromMetadata(ResultSetMetaData metaData, Map<Field, SqlAccessor> fieldToAccessorHolder, SqlService sqlService) {
        try {
            int columnCount = metaData.getColumnCount();
            for (int i = 1; i <= columnCount; i++) {
                String columnName = metaData.getColumnName(i);
                if (!updateSqlIndex(fieldToAccessorHolder, sqlService, i, columnName)) {
                    LOGGER.warn("column " + columnName + " not found in type got " + fieldToAccessorHolder.keySet());
                }
            }
        } catch (SQLException e) {
            String msg = "Fail to analyse metadata of " + sql;
            LOGGER.error(msg, e);
            throw new RuntimeException(msg, e);
        }
    }

    private boolean updateSqlIndex(Map<Field, SqlAccessor> fieldToAccessorHolder, SqlService sqlService, int i, String columnName) {
        for (Map.Entry<Field, SqlAccessor> fieldSqlAccessorEntry : fieldToAccessorHolder.entrySet()) {
            if (sqlService.getColumnName(fieldSqlAccessorEntry.getKey()).equals(columnName)) {
                fieldSqlAccessorEntry.getValue().setIndex(i);
                return true;
            }
        }
        return false;
    }


    private String prepareSqlRequest() {
        int index = 0;
        StringPrettyWriter prettyWriter = new StringPrettyWriter();
        prettyWriter.append("select ");

        for (SqlOperation sqlOperation : sqlOperations) {
            SqlAccessor sqlAccessor = sqlOperation.getAccessor();
            sqlAccessor.setIndex(++index);
            prettyWriter.append(sqlOperation.toSqlOpe(new ToSqlName(){
                public String toSqlName(Field field) {
                    GlobType globType = field.getGlobType();
                    globTypes.add(globType);
                    String tableName = sqlService.getTableName(globType);
                    return tableName + "." + sqlService.getColumnName(field);
                }
            })
            );
            prettyWriter.append(", ");
        }

        for (Map.Entry<Field, SqlAccessor> fieldAndAccessor : fieldToAccessorHolder.entrySet()) {
            fieldAndAccessor.getValue().setIndex(++index);
            Field field = fieldAndAccessor.getKey();
            GlobType globType = field.getGlobType();
            globTypes.add(globType);
            String tableName = sqlService.getTableName(globType);
            if (distinct.contains(field)) {
                prettyWriter.append(" DISTINCT ");
            }
            prettyWriter.append(tableName)
                    .append(".")
                    .append(sqlService.getColumnName(field))
                    .append(", ");
        }
        // remove ", "
        prettyWriter.removeLast().removeLast();
        StringPrettyWriter where = null;
        if (constraint != null) {
            where = new StringPrettyWriter();
            where.append(" WHERE ");
            constraint.visit(new WhereClauseConstraintVisitor(where, sqlService, globTypes));
        }

        prettyWriter.append(" from ");
        for (Iterator it = globTypes.iterator(); it.hasNext(); ) {
            GlobType globType = (GlobType) it.next();
            prettyWriter.append(sqlService.getTableName(globType))
                    .appendIf(", ", it.hasNext());
        }
        if (where != null) {
            prettyWriter.append(where.toString());
        }

        if (!orders.isEmpty()) {
            prettyWriter.append(" ORDER BY ");
            for (SqlQueryBuilder.Order order : orders) {
                prettyWriter.append(sqlService.getColumnName(order.field));
                if (order.asc) {
                    prettyWriter.append(" ASC");
                }
                else {
                    prettyWriter.append(" DESC");
                }
                prettyWriter.append(", ");
            }
            prettyWriter.removeLast().removeLast();
        }
        if (top != -1) {
            prettyWriter.append(" LIMIT " + top);
        }
        return prettyWriter.toString();
    }

    public Stream<?> executeAsStream() {
        DbStream dbStream = execute();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<Object>() {
                    private Boolean hasNext;

                    @Override
                    public boolean hasNext() {
                        if (hasNext == null) {
                            hasNext = dbStream.next();
                        }
                        return hasNext;
                    }

                    @Override
                    public Object next() {
                        hasNext = null;
                        return null;
                    }
                }, 0), false)
                .onClose(this::resultSetClose);
    }

    public Stream<Glob> executeAsGlobStream() {
        DbStream dbStream = execute();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new GlobIterator(dbStream), 0), false)
                .onClose(this::resultSetClose);
    }

    public DbStream execute() {
        if (preparedStatement == null) {
            throw new UnexpectedApplicationState("Query closed " + sql);
        }
        try {
            if (constraint != null) {
                constraint.visit(new ValueConstraintVisitor(preparedStatement, blobUpdater));
            }
            ResultSet resultSet = preparedStatement.executeQuery();
            if (shouldInitAccessorWithMetadata) {
                initIndexFromMetadata(resultSet.getMetaData(), fieldToAccessorHolder, sqlService);
                shouldInitAccessorWithMetadata = false;
            }
            return new SqlDbStream(resultSet, fieldToAccessorHolder,
                    sqlOperations.stream().map(sqlOperation -> sqlOperation.getAccessor()).collect(Collectors.toList()), this);
        } catch (SQLException e) {
            throw new UnexpectedApplicationState("for request : " + sql, e);
        }
    }

    public GlobList executeAsGlobs() {
        DbStream dbStream = execute();
        AccessorGlobBuilder accessorGlobBuilder = AccessorGlobBuilder.init(dbStream);
        GlobList result = new GlobList();
        while (dbStream.next()) {
            result.add(accessorGlobBuilder.getGlob());
        }
        return result;
    }

    public String toString() {
        return sql;
    }

    public void resultSetClose() {
        if (autoClose) {
            close();
        }
    }

    public void close() {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
                preparedStatement = null;
            } catch (SQLException e) {
                throw new UnexpectedApplicationState("PreparedStatement close fail", e);
            }
        }
    }

    private static class GlobIterator implements Iterator<Glob> {
        private AccessorGlobBuilder globsBuilder;
        private DbStream dbStream;
        private Glob current;

        public GlobIterator(DbStream dbStream) {
            this.dbStream = dbStream;
            globsBuilder = AccessorGlobBuilder.init(this.dbStream);
            goToNext();
        }

        public boolean hasNext() {
            return current != null;
        }

        public Glob next() {
            try {
                return current;
            } finally {
                goToNext();
            }
        }

        private void goToNext() {
            if (dbStream.next()) {
                current = globsBuilder.getGlob();
            }
            else {
                current = null;
            }
        }
    }
}
