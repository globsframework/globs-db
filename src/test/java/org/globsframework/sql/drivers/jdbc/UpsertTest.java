package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.model.Glob;
import org.globsframework.sql.CreateBuilder;
import org.globsframework.sql.SqlRequest;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.exceptions.UniqueConstraintViolation;
import org.globsframework.sql.Upsert;
import org.globsframework.sql.drivers.mysql.MysqlConnection;
import org.globsframework.sql.drivers.oracle.OracleConnection;
import org.globsframework.sql.drivers.postgresql.PostgresqlConnection;
import org.globsframework.sql.model.DummyObject;
import org.junit.Test;

import java.sql.Connection;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.*;

/**
 * There was no way to insert a row that may already be there: the statement failed on the duplicate
 * key and the caller had to read first, then choose between an insert and an update.
 */
public class UpsertTest extends DbServicesTestCase {

    @Test
    public void withoutAnUpsertADuplicateStillFails() {
        insert(1, "first", 10);

        assertThrows(UniqueConstraintViolation.class, () -> insert(1, "again", 20));
    }

    @Test
    public void anExistingRowIsUpdated() {
        insert(1, "first", 10);

        upsert(1, "second", 20);

        assertEquals(1, count());
        Glob row = read(1);
        assertEquals("second", row.get(DummyObject.NAME));
        assertEquals(Integer.valueOf(20), row.get(DummyObject.COUNT));
    }

    @Test
    public void anAbsentRowIsInserted() {
        upsert(1, "brand new", 5);

        assertEquals(1, count());
        assertEquals("brand new", read(1).get(DummyObject.NAME));
    }

    @Test
    public void doNothingLeavesTheExistingRowAlone() {
        insert(1, "original", 10);

        try (SqlRequest request = builder(1, "ignored", 99).onConflictDoNothing(DummyObject.ID).getRequest()) {
            request.apply();
        }
        sqlConnection.commit();

        assertEquals(1, count());
        assertEquals("original", read(1).get(DummyObject.NAME));
        assertEquals(Integer.valueOf(10), read(1).get(DummyObject.COUNT));
    }

    @Test
    public void onlyTheNamedColumnsAreOverwritten() {
        insert(1, "original", 10);

        try (SqlRequest request = builder(1, "renamed", 99)
                .onConflictUpdate(List.of(DummyObject.ID), List.of(DummyObject.NAME))
                .getRequest()) {
            request.apply();
        }
        sqlConnection.commit();

        assertEquals("renamed", read(1).get(DummyObject.NAME));
        // count was inserted but left out of the update list
        assertEquals(Integer.valueOf(10), read(1).get(DummyObject.COUNT));
    }

    @Test
    public void theConflictColumnsDefaultToTheKey() {
        insert(1, "first", 10);

        try (SqlRequest request = builder(1, "keyed", 20).onConflictUpdate().getRequest()) {
            request.apply();
        }
        sqlConnection.commit();

        assertEquals(1, count());
        assertEquals("keyed", read(1).get(DummyObject.NAME));
    }

    @Test
    public void oneStatementServesInsertAndUpdateInABatch() {
        insert(1, "existing", 10);

        CreateBuilder builder = sqlConnection.getCreateBuilder(DummyObject.TYPE);
        org.globsframework.core.streams.accessors.utils.ValueIntegerAccessor id =
                new org.globsframework.core.streams.accessors.utils.ValueIntegerAccessor(0);
        org.globsframework.core.streams.accessors.utils.ValueStringAccessor name =
                new org.globsframework.core.streams.accessors.utils.ValueStringAccessor("");
        builder.set(DummyObject.ID, id).set(DummyObject.NAME, name).onConflictUpdate(DummyObject.ID);

        try (org.globsframework.sql.BatchSqlRequest request = builder.getBulkRequest()) {
            id.setValue(1);
            name.setValue("updated");
            request.addBatch();
            id.setValue(2);
            name.setValue("created");
            request.addBatch();
            request.applyBatch();
        }
        sqlConnection.commit();

        assertEquals(2, count());
        assertEquals("updated", read(1).get(DummyObject.NAME));
        assertEquals("created", read(2).get(DummyObject.NAME));
    }

    @Test
    public void aConflictColumnThatIsNotInsertedIsRefused() {
        CreateBuilder builder = sqlConnection.getCreateBuilder(DummyObject.TYPE)
                .set(DummyObject.NAME, "no id")
                .onConflictUpdate(DummyObject.ID);

        IllegalStateException thrown = assertThrows(IllegalStateException.class, builder::getRequest);

        assertTrue(thrown.getMessage(), thrown.getMessage().contains("not"));
    }

    @Test
    public void eachDialectWritesItsOwnStatement() {
        List<Field> columns = List.of(DummyObject.ID, DummyObject.NAME, DummyObject.COUNT);
        Upsert upsert = Upsert.update(List.of(DummyObject.ID), null)
                .resolve(DummyObject.TYPE, columns);
        Connection jdbc = ((JdbcConnection) sqlConnection).getConnection();
        Function<Field, String> placeholder = field -> "?";

        String postgres = new PostgresqlConnection(false, jdbc, sqlService)
                .upsertRequest(DummyObject.TYPE, columns, upsert, placeholder);
        assertTrue(postgres, postgres.startsWith("INSERT INTO "));
        assertTrue(postgres, postgres.contains(" ON CONFLICT ("));
        assertTrue(postgres, postgres.contains(" DO UPDATE SET "));
        assertTrue(postgres, postgres.contains("= EXCLUDED."));

        String mysql = new MysqlConnection(false, jdbc, sqlService)
                .upsertRequest(DummyObject.TYPE, columns, upsert, placeholder);
        assertTrue(mysql, mysql.contains(" ON DUPLICATE KEY UPDATE "));
        assertTrue(mysql, mysql.contains("= VALUES("));

        String oracle = new OracleConnection(false, jdbc, sqlService)
                .upsertRequest(DummyObject.TYPE, columns, upsert, placeholder);
        assertTrue(oracle, oracle.startsWith("MERGE INTO "));
        // Oracle has no standalone VALUES row constructor
        assertTrue(oracle, oracle.contains(" FROM dual) src"));
        assertTrue(oracle, oracle.contains(" WHEN MATCHED THEN UPDATE SET "));
        assertTrue(oracle, oracle.contains(" WHEN NOT MATCHED THEN INSERT ("));

        // one placeholder per column, in the same order, whatever the shape
        for (String sql : List.of(postgres, mysql, oracle)) {
            assertEquals(sql, 3, sql.chars().filter(c -> c == '?').count());
        }
    }

    @Test
    public void doNothingIsExpressedByEveryDialect() {
        List<Field> columns = List.of(DummyObject.ID, DummyObject.NAME);
        Upsert upsert = Upsert.doNothing(List.of(DummyObject.ID)).resolve(DummyObject.TYPE, columns);
        Connection jdbc = ((JdbcConnection) sqlConnection).getConnection();
        Function<Field, String> placeholder = field -> "?";

        assertTrue(new PostgresqlConnection(false, jdbc, sqlService)
                .upsertRequest(DummyObject.TYPE, columns, upsert, placeholder).endsWith(" DO NOTHING"));
        // MySQL cannot say "do nothing", it says "assign the key to itself"
        String mysql = new MysqlConnection(false, jdbc, sqlService)
                .upsertRequest(DummyObject.TYPE, columns, upsert, placeholder);
        assertTrue(mysql, mysql.contains(" ON DUPLICATE KEY UPDATE "));
        assertFalse(mysql, mysql.contains("VALUES(" + sqlService.getColumnName(DummyObject.NAME, true) + ")"));
        // the MERGE simply leaves out its WHEN MATCHED branch
        String oracle = new OracleConnection(false, jdbc, sqlService)
                .upsertRequest(DummyObject.TYPE, columns, upsert, placeholder);
        assertFalse(oracle, oracle.contains("WHEN MATCHED"));
        assertTrue(oracle, oracle.contains("WHEN NOT MATCHED"));
    }

    private CreateBuilder builder(int id, String name, int count) {
        return sqlConnection.getCreateBuilder(DummyObject.TYPE)
                .set(DummyObject.ID, id)
                .set(DummyObject.NAME, name)
                .set(DummyObject.COUNT, count);
    }

    private void insert(int id, String name, int count) {
        try (SqlRequest request = builder(id, name, count).getRequest()) {
            request.apply();
        }
        sqlConnection.commit();
    }

    private void upsert(int id, String name, int count) {
        try (SqlRequest request = builder(id, name, count).onConflictUpdate(DummyObject.ID).getRequest()) {
            request.apply();
        }
        sqlConnection.commit();
    }

    private Glob read(int id) {
        return sqlConnection.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, id))
                .selectAll().getQuery().executeUnique();
    }

    private int count() {
        return sqlConnection.getQueryBuilder(DummyObject.TYPE).selectAll().getQuery().executeAsGlobs().size();
    }
}
