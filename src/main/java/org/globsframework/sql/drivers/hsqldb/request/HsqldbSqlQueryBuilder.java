package org.globsframework.sql.drivers.hsqldb.request;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.hsqldb.HsqldbSqlSelectQuery;
import org.globsframework.sql.drivers.jdbc.BlobUpdater;
import org.globsframework.sql.drivers.jdbc.request.SqlQueryBuilder;

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
