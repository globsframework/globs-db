package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.model.FieldValues;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.utils.DefaultFieldValues;
import org.globsframework.core.streams.GlobStream;
import org.globsframework.core.streams.accessors.Accessor;
import org.globsframework.core.utils.NanoChrono;
import org.globsframework.json.GSonUtils;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.accessors.SqlAccessor;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.impl.ValueConstraintVisitor;
import org.globsframework.sql.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sql.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sql.exceptions.SqlException;
import org.globsframework.sql.utils.StringPrettyWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SqlSelectQuery implements SelectQuery {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlSelectQuery.class);
    protected final Set<GlobType> globTypes = new HashSet<GlobType>();
    private final Constraint constraint;
    private final BlobUpdater blobUpdater;
    private final boolean autoClose;
    private final Map<Field, SqlAccessor> fieldToAccessorHolder;
    protected final SqlService sqlService;
    protected final Set<Field> distinct;
    protected final List<SqlOperation> sqlOperations;
    private GlobType fallBackType;
    private PreparedStatement preparedStatement;
    private final String sql;
    private boolean shouldInitAccessorWithMetadata;

    public SqlSelectQuery(SqlService sqlService, Connection connection, String sql,
                          Map<Field, SqlAccessor> fieldToAccessorHolder, GlobType fallBackType) {
        this.sqlService = sqlService;
        this.fieldToAccessorHolder = new HashMap<>(fieldToAccessorHolder);
        this.fallBackType = fallBackType;
        sqlOperations = Collections.emptyList();
        distinct = Collections.emptySet();
        constraint = null;
        blobUpdater = null;
        autoClose = true;
        this.sql = sql;
        NanoChrono nanoChrono = NanoChrono.start();
        try {
            this.preparedStatement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Preparing " + sql + " took " + nanoChrono.getElapsedTimeInMS() + " ms.");
            }
        } catch (SQLException e) {
            String message = "for request " + sql;
            LOGGER.error(message);
            throw new SqlException(message, e);
        }
        shouldInitAccessorWithMetadata = true;
    }

    public SqlSelectQuery(Connection connection, Constraint constraint,
                          Map<Field, SqlAccessor> fieldToAccessorHolder, SqlService sqlService,
                          BlobUpdater blobUpdater, boolean autoClose, List<SqlQueryBuilder.Order> orders,
                          List<Field> groupBy, int top, int skip, Set<Field> distinct, List<SqlOperation> sqlOperations,
                          GlobType fallBackType) {
        this.constraint = constraint;
        this.blobUpdater = blobUpdater;
        this.autoClose = autoClose;
        this.fieldToAccessorHolder = new HashMap<>(fieldToAccessorHolder);
        this.sqlService = sqlService;
        this.distinct = distinct;
        this.sqlOperations = sqlOperations;
        this.fallBackType = fallBackType;
        sql = prepareSqlRequest(top, skip, orders, groupBy);
        NanoChrono nanoChrono = NanoChrono.start();
        try {
            this.preparedStatement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Preparing " + sql + " took " + nanoChrono.getElapsedTimeInMS() + " ms.");
            }
        } catch (SQLException e) {
            String message = "for request " + sql;
            LOGGER.error(message);
            throw new SqlException(message, e);
        }
        shouldInitAccessorWithMetadata = false;
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
            if (sqlService.getColumnName(fieldSqlAccessorEntry.getKey(), true).equals(columnName)) {
                fieldSqlAccessorEntry.getValue().setIndex(i);
                return true;
            }
        }
        return false;
    }

    protected WhereClauseConstraintVisitor getWhereConstraintVisitor(StringPrettyWriter where) {
        return new WhereClauseConstraintVisitor(where, sqlService, globTypes);
    }

    private String prepareSqlRequest(int top, int skip, List<SqlQueryBuilder.Order> orders, List<Field> groupBy) {
        int index = 0;
        StringPrettyWriter prettyWriter = new StringPrettyWriter();
        prettyWriter.append("select ");

        for (SqlOperation sqlOperation : sqlOperations) {
            SqlAccessor sqlAccessor = sqlOperation.getAccessor();
            sqlAccessor.setIndex(++index);
            prettyWriter.append(sqlOperation.toSqlOpe(new ToSqlName() {
                        public String toSqlName(Field field) {
                            GlobType globType = field.getGlobType();
                            globTypes.add(globType);
                            String tableName = sqlService.getTableName(globType, true);
                            return tableName + "." + sqlService.getColumnName(field, true);
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
            String tableName = sqlService.getTableName(globType, true);
            if (distinct.contains(field)) {
                prettyWriter.append(" DISTINCT ");
            }
            prettyWriter.append(tableName)
                    .append(".")
                    .append(sqlService.getColumnName(field, true))
                    .append(", ");
        }
        // remove ", "
        prettyWriter.removeLast(2);
        StringPrettyWriter where = null;
        if (constraint != null) {
            where = new StringPrettyWriter();
            where.append(" WHERE ");
            constraint.accept(getWhereConstraintVisitor(where));
        }

        prettyWriter.append(" from ");
        for (Iterator<GlobType> it = globTypes.iterator(); it.hasNext(); ) {
            GlobType globType = it.next();
            prettyWriter.append(sqlService.getTableName(globType, true))
                    .appendIf(", ", it.hasNext());
        }
        if (where != null) {
            prettyWriter.append(where.toString());
        }

        if (!groupBy.isEmpty()) {
            prettyWriter.append(" GROUP BY ");
            for (Field field : groupBy) {
                String tableName = sqlService.getTableName(field.getGlobType(), true);
                prettyWriter.append(tableName)
                        .append(".")
                        .append(sqlService.getColumnName(field, true))
                        .append(", ");
            }
            prettyWriter.removeLast(2);
        }

        if (!orders.isEmpty()) {
            prettyWriter.append(" ORDER BY ");
            for (SqlQueryBuilder.Order order : orders) {
                prettyWriter.append(sqlService.getColumnName(order.field, true));
                if (order.asc) {
                    prettyWriter.append(" ASC");
                } else {
                    prettyWriter.append(" DESC");
                }
                prettyWriter.append(", ");
            }
            prettyWriter.removeLast(2);
        }
        if (top != -1) {
            prettyWriter.append(" LIMIT " + top);
        }
        if (skip != -1) {
            prettyWriter.append(" OFFSET " + skip);
        }
        return prettyWriter.toString();
    }

    public Stream<?> executeAsStream() {
        GlobStream globStream = execute();
        final DbStreamIterator iterator = new DbStreamIterator(globStream);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
                .onClose(() -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.info("read " + iterator.count() + " elements");
                    }
                })
                .onClose(this::resultSetClose);
    }

    public Stream<Glob> executeAsGlobStream() {
        GlobStream globStream = execute();
        final GlobIterator iterator = new GlobIterator(globStream, fallBackType);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
                .onClose(() -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("read " + iterator.count + " elements.");
                    }
                })
                .onClose(this::resultSetClose);
    }

    public Stream<FieldValues> executeAsFieldValuesStream() {
        GlobStream globStream = execute();
        final FieldValuesIterator iterator = new FieldValuesIterator(globStream);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false)
                .onClose(() -> {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug("read " + iterator.count + " elements.");
                    }
                })
                .onClose(this::resultSetClose);
    }

    public GlobStream execute() {
        if (preparedStatement == null) {
            String message = "Query closed " + sql;
            LOGGER.error(message);
            throw new SqlException(message);
        }
        try {
            if (constraint != null) {
                constraint.accept(new ValueConstraintVisitor(preparedStatement, blobUpdater));
            }
            NanoChrono nanoChrono = NanoChrono.start();
            ResultSet resultSet = preparedStatement.executeQuery();
            if (shouldInitAccessorWithMetadata) {
                initIndexFromMetadata(resultSet.getMetaData(), fieldToAccessorHolder, sqlService);
                shouldInitAccessorWithMetadata = false;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Execution of " + sql + " took " + nanoChrono.getElapsedTimeInMS() + " ms.");
            }
            return new SqlGlobStream(resultSet, fieldToAccessorHolder,
                    sqlOperations.stream().map(SqlOperation::getAccessor).collect(Collectors.toList()), this);
        } catch (SQLException e) {
            String message = "for request : " + sql;
            LOGGER.error(message, e);
            throw new SqlException(message, e);
        }
    }

    public List<Glob> executeAsGlobs() {
        List<Glob> result = new ArrayList<>();
        try (Stream<Glob> globStream = executeAsGlobStream()) {
            globStream.forEach(result::add);
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
                String msg = "PreparedStatement close fail";
                throw new SqlException(msg, e);
            }
        }
    }

    private static class GlobIterator implements Iterator<Glob> {
        private AccessorGlobBuilder globsBuilder;
        private GlobStream globStream;
        private Glob current;
        int count = 0;

        public GlobIterator(GlobStream globStream, GlobType fallBackType) {
            this.globStream = globStream;
            globsBuilder = AccessorGlobBuilder.init(this.globStream, fallBackType);
            goToNext();
        }

        public boolean hasNext() {
            return current != null;
        }

        public Glob next() {
            try {
                count++;
                return current;
            } finally {
                goToNext();
            }
        }

        private void goToNext() {
            if (globStream.next()) {
                current = globsBuilder.getGlob();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("load " + GSonUtils.encode(current, true));
                }
            } else {
                current = null;
            }
        }
    }

    private static class FieldValuesIterator implements Iterator<FieldValues> {
        private GlobStream globStream;
        private Acc[] accs;
        private FieldValues current;
        int count = 0;

        static class Acc{
        public final Field field;
        public final Accessor accessor;

            Acc(Field field, Accessor accessor) {
                this.field = field;
                this.accessor = accessor;
            }
        }

        public FieldValuesIterator(GlobStream globStream) {
            this.globStream = globStream;
            accs = new Acc[globStream.getFields().size()];
            int i = 0;
            for (Field field : globStream.getFields()) {
                final Accessor accessor = globStream.getAccessor(field);
                accs[i++] = new Acc(field, accessor);
            }
            goToNext();
        }

        FieldValues create() {
            DefaultFieldValues defaultFieldValues = new DefaultFieldValues();
            for (Acc acc : accs) {
                defaultFieldValues.setValue(acc.field, acc.accessor.getObjectValue());
            }
            return defaultFieldValues;
        }

        public boolean hasNext() {
            return current != null;
        }

        public FieldValues next() {
            try {
                count++;
                return current;
            } finally {
                goToNext();
            }
        }

        private void goToNext() {
            if (globStream.next()) {
                current = create();
            } else {
                current = null;
            }
        }
    }

    private static class DbStreamIterator implements Iterator<Object> {
        private static final Object NULL = new Object();
        private final GlobStream globStream;
        private int count = 0;
        private Boolean hasNext;

        public DbStreamIterator(GlobStream globStream) {
            this.globStream = globStream;
        }

        public boolean hasNext() {
            if (hasNext == null) {
                hasNext = globStream.next();
            }
            return hasNext;
        }

        public Object next() {
            hasNext = null;
            count++;
            return NULL;
        }

        public int count() {
            return count;
        }
    }
}
