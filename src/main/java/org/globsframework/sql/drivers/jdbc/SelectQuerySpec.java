package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.accessors.SqlAccessor;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.request.SqlQueryBuilder;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Everything a SELECT is built from, apart from the connection.
 * <p>
 * It exists because every dialect's query class forwarded the same twelve arguments to
 * {@link SqlSelectQuery} unchanged: adding one meant editing five query classes and five builders,
 * and the next addition cost the same again. A builder now hands over one of these, and a new
 * setting is a new component here plus the line in {@link SqlQueryBuilder} that fills it.
 *
 * @param top          -1 for no row limit, {@code skip} likewise for no offset
 * @param fallBackType the type globs are built from when no selected field names one
 * @param fetchSize    rows the driver buffers per round trip, 0 to leave the driver's default. On
 *                     PostgreSQL this only streams inside a transaction: an auto-commit connection
 *                     reads the whole result set whatever the value
 * @param queryTimeout how long the statement may run, null for no limit
 */
public record SelectQuerySpec(Constraint constraint,
                              Map<Field, SqlAccessor> fieldToAccessorHolder,
                              SqlService sqlService,
                              boolean autoClose,
                              List<SqlQueryBuilder.Order> orders,
                              List<Field> groupBy,
                              int top,
                              int skip,
                              Set<Field> distinct,
                              List<SqlOperation> sqlOperations,
                              GlobType fallBackType,
                              int fetchSize,
                              Duration queryTimeout) {

    /**
     * The shape the query classes took before fetch size and timeout existed.
     */
    public SelectQuerySpec(Constraint constraint, Map<Field, SqlAccessor> fieldToAccessorHolder,
                           SqlService sqlService, boolean autoClose, List<SqlQueryBuilder.Order> orders,
                           List<Field> groupBy, int top, int skip, Set<Field> distinct,
                           List<SqlOperation> sqlOperations, GlobType fallBackType) {
        this(constraint, fieldToAccessorHolder, sqlService, autoClose, orders, groupBy, top, skip,
                distinct, sqlOperations, fallBackType, 0, null);
    }
}
