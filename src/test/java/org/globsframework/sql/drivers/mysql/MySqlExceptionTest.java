package org.globsframework.sql.drivers.mysql;

import org.globsframework.sql.SqlConnection;
import org.globsframework.sql.drivers.jdbc.JdbcSqlService;
import org.globsframework.sql.drivers.jdbc.SqlExceptionTest;

public abstract class MySqlExceptionTest extends SqlExceptionTest {
    public SqlConnection getDb() {
        return new JdbcSqlService("jdbc:mysql://localhost/test", "sa", "").getDb();
    }
}
