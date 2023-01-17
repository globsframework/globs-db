package org.globsframework.sqlstreams.drivers.hsqldb.request;

import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.hsqldb.HsqldbSqlSelectQuery;
import org.globsframework.sqlstreams.drivers.jdbc.BlobUpdater;
import org.globsframework.sqlstreams.drivers.jdbc.request.SqlQueryBuilder;

import java.sql.Connection;

public class HsqldbSqlQueryBuilder extends SqlQueryBuilder {

    public HsqldbSqlQueryBuilder(Connection connection, GlobType globType, Constraint constraint, SqlService sqlService, BlobUpdater blobUpdater) {
        super(connection, globType, constraint, sqlService, blobUpdater);
    }

    public SelectQuery getQuery() {
        try {
            return new HsqldbSqlSelectQuery(connection, constraint, fieldToAccessorHolder, sqlService, blobUpdater, autoClose,
                    orders, groupBy, top, skip, distinct, sqlOperations, fallBackType);
        } finally {
            fieldToAccessorHolder.clear();
        }
    }

}
