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
import org.globsframework.sql.Join;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.TableRef;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.accessors.SqlAccessor;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.impl.ValueConstraintVisitor;
import org.globsframework.sql.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sql.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sql.exceptions.SqlException;
import org.globsframework.sql.exceptions.SqlExceptions;
import org.globsframework.sql.utils.StringPrettyWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class SqlSelectQuery implements SelectQuery {
    private static final Logger LOGGER = LoggerFactory.getLogger(SqlSelectQuery.class);
    protected final Set<GlobType> globTypes = new HashSet<GlobType>();
    private final Constraint constraint;
    private final boolean autoClose;
    private final Map<Field, SqlAccessor> fieldToAccessorHolder;
    protected final SqlService sqlService;
    protected final Set<Field> distinct;
    protected final List<SqlOperation> sqlOperations;
    private GlobType fallBackType;
    private PreparedStatement preparedStatement;
    private final String sql;
    private boolean shouldInitAccessorWithMetadata;
    private final List<SqlAccessor> additionalAccessor;
    private final TableRef rootTable;
    private final List<Join> joins;
    private final Map<Field, TableRef> columnTables;
    private final ColumnQualifier columnQualifier;

    public SqlSelectQuery(SqlService sqlService, Connection connection, String sql,
                          Map<Field, SqlAccessor> fieldToAccessorHolder, GlobType fallBackType) {
        this(sqlService, connection, sql, fieldToAccessorHolder, fallBackType, 0, null);
    }

    public SqlSelectQuery(SqlService sqlService, Connection connection, String sql,
                          Map<Field, SqlAccessor> fieldToAccessorHolder, GlobType fallBackType,
                          int fetchSize, Duration queryTimeout) {
        this.sqlService = sqlService;
        this.fieldToAccessorHolder = new HashMap<>(fieldToAccessorHolder);
        this.fallBackType = fallBackType;
        sqlOperations = List.of();
        distinct = Set.of();
        constraint = null;
        autoClose = true;
        rootTable = null;
        joins = List.of();
        columnTables = Map.of();
        columnQualifier = ColumnQualifier.byTableName(sqlService, globTypes);
        this.sql = sql;
        this.preparedStatement = prepare(connection, sql, fetchSize, queryTimeout);
        additionalAccessor = List.of();
        shouldInitAccessorWithMetadata = true;
    }

    /**
     * @deprecated the arguments are a {@link SelectQuerySpec} now, so that a new setting does not
     * mean a new parameter in every dialect's query class.
     */
    @Deprecated
    public SqlSelectQuery(Connection connection, Constraint constraint,
                          Map<Field, SqlAccessor> fieldToAccessorHolder, SqlService sqlService,
                          boolean autoClose, List<SqlQueryBuilder.Order> orders,
                          List<Field> groupBy, int top, int skip, Set<Field> distinct, List<SqlOperation> sqlOperations,
                          GlobType fallBackType) {
        this(connection, new SelectQuerySpec(constraint, fieldToAccessorHolder, sqlService, autoClose,
                orders, groupBy, top, skip, distinct, sqlOperations, fallBackType));
    }

    public SqlSelectQuery(Connection connection, SelectQuerySpec spec) {
        this.constraint = spec.constraint();
        this.autoClose = spec.autoClose();
        this.fieldToAccessorHolder = new HashMap<>(spec.fieldToAccessorHolder());
        this.sqlService = spec.sqlService();
        this.distinct = spec.distinct();
        this.sqlOperations = spec.sqlOperations();
        this.fallBackType = spec.fallBackType();
        this.rootTable = spec.rootTable();
        this.joins = spec.joins();
        this.columnTables = spec.columnTables();
        // asking the builder for a TableRef -- to join, or to correlate a subquery -- is what puts
        // the query in alias mode; one that never does generates exactly the SQL it always did
        this.columnQualifier = rootTable == null
                ? ColumnQualifier.byTableName(sqlService, globTypes)
                : aliasQualifier();
        sql = prepareSqlRequest(spec.top(), spec.skip(), spec.orders(), spec.groupBy());
        this.preparedStatement = prepare(connection, sql, spec.fetchSize(), spec.queryTimeout());
        additionalAccessor = sqlOperations.stream().map(SqlOperation::getAccessor).collect(Collectors.toList());
        shouldInitAccessorWithMetadata = false;
    }

    private static PreparedStatement prepare(Connection connection, String sql, int fetchSize,
                                             Duration queryTimeout) {
        NanoChrono nanoChrono = NanoChrono.start();
        try {
            PreparedStatement statement = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                    ResultSet.CONCUR_READ_ONLY);
            if (fetchSize > 0) {
                statement.setFetchSize(fetchSize);
            }
            if (queryTimeout != null) {
                // JDBC counts seconds; anything shorter than a second would round down to "no limit"
                statement.setQueryTimeout(Math.max(1, (int) Math.min(Integer.MAX_VALUE, queryTimeout.toSeconds())));
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Preparing " + sql + " took " + nanoChrono.getElapsedTimeInMS() + " ms.");
            }
            return statement;
        } catch (SQLException e) {
            String message = "for request " + sql;
            LOGGER.error(message);
            throw SqlExceptions.typed(message, e);
        }
    }

    /**
     * Resolves a column to the alias of its occurrence. A bare field is allowed as long as its type
     * is joined exactly once, which is what keeps every existing query working; a type joined twice
     * has to be named, since nothing else can say which side is meant.
     */
    private ColumnQualifier aliasQualifier() {
        Map<GlobType, TableRef> single = new HashMap<>();
        Set<GlobType> ambiguous = new HashSet<>();
        List<TableRef> occurrences = new ArrayList<>();
        occurrences.add(rootTable);
        joins.forEach(join -> occurrences.add(join.table()));
        for (TableRef occurrence : occurrences) {
            if (single.putIfAbsent(occurrence.getType(), occurrence) != null) {
                ambiguous.add(occurrence.getType());
            }
        }
        return (field, table) -> {
            if (table != null) {
                return table.getAlias();
            }
            GlobType type = field.getGlobType();
            if (ambiguous.contains(type)) {
                throw new SqlException(field.getFullName() + " is ambiguous: " + type.getName()
                        + " appears more than once in the query, name the occurrence with TableRef.column(...)");
            }
            TableRef occurrence = single.get(type);
            if (occurrence == null) {
                throw new SqlException(field.getFullName() + " belongs to " + type.getName()
                        + ", which this query does not join");
            }
            return occurrence.getAlias();
        };
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
            // unescaped: ResultSetMetaData reports the name as the database stores it, never quoted
            if (sqlService.getColumnName(fieldSqlAccessorEntry.getKey(), false).equals(columnName)) {
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
                            return toSqlName(field, null);
                        }

                        public String toSqlName(Field field, TableRef table) {
                            return columnQualifier.qualify(field, table) + "."
                                    + sqlService.getColumnName(field, true);
                        }
                    })
            );
            prettyWriter.append(", ");
        }

        for (Map.Entry<Field, SqlAccessor> fieldAndAccessor : fieldToAccessorHolder.entrySet()) {
            fieldAndAccessor.getValue().setIndex(++index);
            Field field = fieldAndAccessor.getKey();
            String tableName = columnQualifier.qualify(field, columnTables.get(field));
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
            constraint.accept(qualifiedWhereVisitor(where));
        }

        prettyWriter.append(" from ");
        if (rootTable == null) {
            if (globTypes.isEmpty()) {
                globTypes.add(fallBackType);
            }
            for (Iterator<GlobType> it = globTypes.iterator(); it.hasNext(); ) {
                GlobType globType = it.next();
                prettyWriter.append(sqlService.getTableName(globType, true))
                        .appendIf(", ", it.hasNext());
            }
        } else {
            appendTable(prettyWriter, rootTable);
            for (Join join : joins) {
                prettyWriter.append(" ").append(join.kind().toSql()).append(" ");
                appendTable(prettyWriter, join.table());
                prettyWriter.append(" ON (");
                join.on().accept(qualifiedWhereVisitor(prettyWriter));
                prettyWriter.append(")");
            }
        }
        if (where != null) {
            prettyWriter.append(where.toString());
        }

        if (!groupBy.isEmpty()) {
            prettyWriter.append(" GROUP BY ");
            for (Field field : groupBy) {
                String tableName = columnQualifier.qualify(field, columnTables.get(field));
                prettyWriter.append(tableName)
                        .append(".")
                        .append(sqlService.getColumnName(field, true))
                        .append(", ");
            }
            prettyWriter.removeLast(2);
        }

        if (!orders.isEmpty()) {
            prettyWriter.append(" ORDER BY ");
            // unqualified where a single table leaves no doubt, which is what it always did; with
            // several it is ambiguous, and a database is entitled to refuse it
            boolean qualify = rootTable != null || globTypes.size() > 1;
            for (SqlQueryBuilder.Order order : orders) {
                if (qualify) {
                    TableRef table = order.table != null ? order.table : columnTables.get(order.field);
                    prettyWriter.append(columnQualifier.qualify(order.field, table)).append(".");
                }
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
        appendTopAndSkip(prettyWriter, top, skip);
        return prettyWriter.toString();
    }

    private void appendTable(StringPrettyWriter writer, TableRef table) {
        // no AS before a table alias: Oracle accepts it only in front of a column alias
        writer.append(sqlService.getTableName(table.getType(), true)).append(" ").append(table.getAlias());
    }

    private WhereClauseConstraintVisitor qualifiedWhereVisitor(StringPrettyWriter writer) {
        WhereClauseConstraintVisitor visitor = getWhereConstraintVisitor(writer);
        visitor.setColumnQualifier(columnQualifier);
        return visitor;
    }

    /**
     * Appends the row limiting clause. Overridden by drivers whose dialect does not know LIMIT/OFFSET.
     */
    protected void appendTopAndSkip(StringPrettyWriter prettyWriter, int top, int skip) {
        if (top != -1) {
            prettyWriter.append(" LIMIT " + top);
        }
        if (skip != -1) {
            prettyWriter.append(" OFFSET " + skip);
        }
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
        // the ON conditions come before the WHERE in the statement, so their values bind first
        int index = 0;
        for (Join join : joins) {
            ValueConstraintVisitor visitor = new ValueConstraintVisitor(preparedStatement, index,
                    sqlService.getNativeValueBinder());
            join.on().accept(visitor);
            index = visitor.getIndex();
        }
        if (constraint != null) {
            constraint.accept(new ValueConstraintVisitor(preparedStatement, index,
                    sqlService.getNativeValueBinder()));
        }
        long start = System.nanoTime();
        try {
            ResultSet resultSet = preparedStatement.executeQuery();
            if (shouldInitAccessorWithMetadata) {
                initIndexFromMetadata(resultSet.getMetaData(), fieldToAccessorHolder, sqlService);
                shouldInitAccessorWithMetadata = false;
            }
            // the rows have not been walked yet, so there is no count to report
            sqlService.getListener().onStatement(sql, System.nanoTime() - start, -1, null);
            return new SqlGlobStream(resultSet, fieldToAccessorHolder, additionalAccessor, this);
        } catch (SQLException e) {
            String message = "for request : " + sql;
            LOGGER.error(message, e);
            SqlException typed = SqlExceptions.typed(message, e);
            sqlService.getListener().onStatement(sql, System.nanoTime() - start, -1, typed);
            throw typed;
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

        record Acc(Field field, Accessor accessor) {

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
