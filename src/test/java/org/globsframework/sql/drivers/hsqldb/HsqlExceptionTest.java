package org.globsframework.sql.drivers.hsqldb;

import org.globsframework.sql.SqlConnection;
import org.globsframework.sql.drivers.jdbc.JdbcSqlService;
import org.globsframework.sql.drivers.jdbc.SqlExceptionTest;

public abstract class HsqlExceptionTest extends SqlExceptionTest {
    public SqlConnection getDb() {
        return new JdbcSqlService("jdbc:hsqldb:.", "sa", "").getDb();
    }
}