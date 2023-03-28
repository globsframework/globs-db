package org.globsframework.sqlstreams.drivers.postgresql;

import org.globsframework.metamodel.DummyObject2;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcSqlService;
import org.globsframework.sqlstreams.drivers.jdbc.SqlExceptionTest;
import org.globsframework.sqlstreams.exceptions.ConstraintViolation;

public abstract class PostgresqlExceptionTest extends SqlExceptionTest {
    public SqlConnection getDb() {
        return new JdbcSqlService("jdbc:postgresql://localhost/test", "sa", "").getDb();
    }

    public static void main(String[] args) {
        SqlConnection db1 = new JdbcSqlService("jdbc:postgresql://localhost:5432/shopify_db",
                "xxx", "www").getDb();
        db1.createTable(DummyObject2.TYPE);
        db1.getDeleteRequest(DummyObject2.TYPE, Constraints.equal(DummyObject2.ID, 1)).run();
        db1.getCreateBuilder(DummyObject2.TYPE).set(DummyObject2.ID, 1).getRequest().run();
        db1.commit();
        try {
            db1.getCreateBuilder(DummyObject2.TYPE).set(DummyObject2.ID, 1).getRequest().run();
        } catch (ConstraintViolation e) {
            System.out.println("PostgresqlExceptionTest.main ignore duplicate");
        } catch (Exception exception) {
            throw new RuntimeException(exception);
        }
        db1.commit();

    }

}
