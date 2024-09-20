package org.globsframework.sql.drivers.postgresql.request;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.BlobUpdater;
import org.globsframework.sql.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sql.drivers.postgresql.PostgreSqlSelectQuery;

import java.sql.Connection;

public class PostgreSqlQueryBuilder extends SqlQueryBuilder {
    public PostgreSqlQueryBuilder(Connection connection, GlobType globType, Constraint constraint, SqlService sqlService, BlobUpdater blobUpdater) {
        super(connection, globType, constraint, sqlService, blobUpdater);
    }

    public SelectQuery getQuery() {
        try {
            return new PostgreSqlSelectQuery(connection, constraint, fieldToAccessorHolder, sqlService, blobUpdater, autoClose,
                    orders, groupBy, top, skip, distinct, sqlOperations, fallBackType);
        } finally {
            fieldToAccessorHolder.clear();
        }
    }
}
