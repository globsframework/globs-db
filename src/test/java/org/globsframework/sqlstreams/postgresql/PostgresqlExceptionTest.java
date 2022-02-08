package org.globsframework.sqlstreams.postgresql;

import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcSqlService;
import org.globsframework.sqlstreams.drivers.jdbc.SqlExceptionTest;

public abstract class PostgresqlExceptionTest extends SqlExceptionTest {
    public SqlConnection getDb() {
        return new JdbcSqlService("jdbc:postgresql://localhost/test", "sa", "").getDb();
    }



}
