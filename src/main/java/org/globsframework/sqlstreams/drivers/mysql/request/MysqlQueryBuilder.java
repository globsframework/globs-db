package org.globsframework.sqlstreams.drivers.mysql.request;

import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.jdbc.BlobUpdater;
import org.globsframework.sqlstreams.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sqlstreams.drivers.mysql.MysqlSelectQuery;

import java.sql.Connection;

public class MysqlQueryBuilder extends SqlQueryBuilder {
    public MysqlQueryBuilder(Connection connection, GlobType globType, Constraint constraint, SqlService sqlService, BlobUpdater blobUpdater) {
        super(connection, globType, constraint, sqlService, blobUpdater);
    }

    public SelectQuery getQuery() {
        try {
            return new MysqlSelectQuery(connection, constraint, fieldToAccessorHolder, sqlService, blobUpdater, autoClose,
                    orders, groupBy, top, distinct, sqlOperations);
        } finally {
            fieldToAccessorHolder.clear();
        }
    }
}