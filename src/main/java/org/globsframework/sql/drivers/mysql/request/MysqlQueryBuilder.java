package org.globsframework.sql.drivers.mysql.request;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sql.drivers.mysql.MysqlSelectQuery;

import java.sql.Connection;

public class MysqlQueryBuilder extends SqlQueryBuilder {
    public MysqlQueryBuilder(Connection connection, GlobType globType, Constraint constraint, SqlService sqlService) {
        super(connection, globType, constraint, sqlService);
    }

    public SelectQuery getQuery() {
        try {
            return new MysqlSelectQuery(connection, constraint, fieldToAccessorHolder, sqlService, autoClose,
                    orders, groupBy, top, skip, distinct, sqlOperations, fallBackType == null ? globType : fallBackType);
        } finally {
            fieldToAccessorHolder.clear();
        }
    }
}
