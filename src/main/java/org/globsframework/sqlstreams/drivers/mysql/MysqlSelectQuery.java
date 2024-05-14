package org.globsframework.sqlstreams.drivers.mysql;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.accessors.SqlAccessor;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.jdbc.BlobUpdater;
import org.globsframework.sqlstreams.drivers.jdbc.SqlOperation;
import org.globsframework.sqlstreams.drivers.jdbc.SqlSelectQuery;
import org.globsframework.sqlstreams.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sqlstreams.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sqlstreams.drivers.mysql.impl.MysqlWhereClauseConstraintVisitor;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;

import java.sql.Connection;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MysqlSelectQuery  extends SqlSelectQuery {

    public MysqlSelectQuery(Connection connection, Constraint constraint, Map<Field, SqlAccessor> fieldToAccessorHolder, SqlService sqlService, BlobUpdater blobUpdater, boolean autoClose, List<SqlQueryBuilder.Order> orders, List<Field> groupBy, int top, int skip, Set<Field> distinct, List<SqlOperation> sqlOperations, GlobType fallBackType) {
        super(connection, constraint, fieldToAccessorHolder, sqlService, blobUpdater, autoClose, orders, groupBy, top, skip, distinct, sqlOperations, fallBackType);
    }

    protected WhereClauseConstraintVisitor getWhereConstraintVisitor(StringPrettyWriter where) {
        return new MysqlWhereClauseConstraintVisitor(where, sqlService, globTypes);
    }
}
