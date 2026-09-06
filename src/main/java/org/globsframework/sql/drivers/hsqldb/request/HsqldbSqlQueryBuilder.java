package org.globsframework.sql.drivers.hsqldb.request;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.hsqldb.HsqldbSqlSelectQuery;
import org.globsframework.sql.drivers.jdbc.request.SqlQueryBuilder;

import java.sql.Connection;

public class HsqldbSqlQueryBuilder extends SqlQueryBuilder {

    public HsqldbSqlQueryBuilder(Connection connection, GlobType globType, Constraint constraint, SqlService sqlService) {
        super(connection, globType, constraint, sqlService);
    }

    public SelectQuery getQuery() {
        try {
            return new HsqldbSqlSelectQuery(connection, spec());
        } finally {
            fieldToAccessorHolder.clear();
        }
    }

}
