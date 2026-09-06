package org.globsframework.sql.drivers.oracle.request;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.request.SqlQueryBuilder;
import org.globsframework.sql.drivers.oracle.OracleSelectQuery;

import java.sql.Connection;

public class OracleSqlQueryBuilder extends SqlQueryBuilder {
    public OracleSqlQueryBuilder(Connection connection, GlobType globType, Constraint constraint, SqlService sqlService) {
        super(connection, globType, constraint, sqlService);
    }

    public SelectQuery getQuery() {
        try {
            return new OracleSelectQuery(connection, spec());
        } finally {
            fieldToAccessorHolder.clear();
        }
    }
}
