package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.model.Glob;
import org.globsframework.sql.SqlRequest;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.drivers.hsqldb.HsqlConnection;
import org.globsframework.sql.drivers.mysql.MysqlConnection;
import org.globsframework.sql.drivers.postgresql.PostgresqlConnection;
import org.globsframework.sql.model.DummyNativeTypes;
import org.globsframework.sql.model.DummyObject2;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.util.UUID;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * A column can be the database's own type rather than a string: uuid and jsonb on PostgreSQL, JSON
 * on MySQL, plus a verbatim escape hatch. The Java side does not move — the field stays a String, or
 * a Glob the library encodes — only the column type and the way the value is bound.
 */
public class NativeTypeTest extends DbServicesTestCase {
    private static final String REFERENCE = "6b1d3c9a-4b5e-4f3a-9c2d-8e7f1a2b3c4d";

    @Before
    public void createNativeTable() {
        sqlConnection.createTable(DummyNativeTypes.TYPE);
        sqlConnection.getDeleteRequest(DummyNativeTypes.TYPE).apply();
        sqlConnection.commit();
    }

    @Test
    public void valuesRoundTripThroughTheNativeColumns() {
        insert(1, REFERENCE, "{\"a\":1}");

        Glob row = read(1);

        assertEquals(REFERENCE, row.get(DummyNativeTypes.REFERENCE));
        assertTrue(row.get(DummyNativeTypes.PAYLOAD), row.get(DummyNativeTypes.PAYLOAD).contains("\"a\""));
        assertEquals("keep me", row.get(DummyNativeTypes.VERBATIM));
        assertEquals(Long.valueOf(7), row.get(DummyNativeTypes.NESTED).get(DummyObject2.ID));
    }

    @Test
    public void aNativeColumnCanBeFilteredOn() {
        insert(1, REFERENCE, "{\"a\":1}");
        insert(2, UUID.randomUUID().toString(), "{\"a\":2}");

        // binding a string against a uuid column is exactly what PostgreSQL refuses unless the
        // parameter is sent untyped
        Glob found = sqlConnection.getQueryBuilder(DummyNativeTypes.TYPE,
                        Constraints.equal(DummyNativeTypes.REFERENCE, REFERENCE))
                .selectAll().getQuery().executeUnique();

        assertEquals(1, found.get(DummyNativeTypes.ID).intValue());
    }

    @Test
    public void aNativeColumnCanBeUpdated() {
        insert(1, REFERENCE, "{\"a\":1}");
        String other = UUID.randomUUID().toString();

        sqlConnection.getUpdateBuilder(DummyNativeTypes.TYPE,
                        Constraints.equal(DummyNativeTypes.ID, 1))
                .update(DummyNativeTypes.REFERENCE, other)
                .update(DummyNativeTypes.PAYLOAD, "{\"a\":42}")
                .getRequest().apply();
        sqlConnection.commit();

        assertEquals(other, read(1).get(DummyNativeTypes.REFERENCE));
        assertTrue(read(1).get(DummyNativeTypes.PAYLOAD).contains("42"));
    }

    @Test
    public void aNullNativeValueIsStillNull() {
        try (SqlRequest request = sqlConnection.getCreateBuilder(DummyNativeTypes.TYPE)
                .set(DummyNativeTypes.ID, 3)
                .set(DummyNativeTypes.REFERENCE, (String) null)
                .set(DummyNativeTypes.PAYLOAD, (String) null)
                .getRequest()) {
            request.apply();
        }
        sqlConnection.commit();

        assertEquals(null, read(3).get(DummyNativeTypes.REFERENCE));
    }

    @Test
    public void eachDialectNamesItsOwnTypes() {
        Connection jdbc = ((JdbcConnection) sqlConnection).getConnection();

        String postgres = new PostgresqlConnection(false, jdbc, sqlService)
                .createTableRequest(DummyNativeTypes.TYPE);
        assertTrue(postgres, postgres.contains(" uuid"));
        assertTrue(postgres, postgres.contains(" jsonb"));
        // the composite field goes to jsonb too, being JSON already
        assertEquals(postgres, 2, countOf(postgres, "jsonb"));
        assertTrue(postgres, postgres.contains(" VARCHAR(64)"));

        String mysql = new MysqlConnection(false, jdbc, sqlService)
                .createTableRequest(DummyNativeTypes.TYPE);
        assertTrue(mysql, mysql.contains(" JSON"));
        // MySQL has no uuid type: the canonical 36 character form
        assertTrue(mysql, mysql.contains(" CHAR(36)"));

        // no dedicated type in HSQLDB: the canonical form and plain text
        String hsqldb = new HsqlConnection(false, jdbc, sqlService)
                .createTableRequest(DummyNativeTypes.TYPE);
        assertTrue(hsqldb, hsqldb.contains(" CHAR(36)"));
        assertFalse(hsqldb, hsqldb.contains("jsonb"));
        assertTrue(hsqldb, hsqldb.contains(" VARCHAR(64)"));
    }

    private int countOf(String sql, String type) {
        int count = 0;
        for (int i = sql.indexOf(type); i >= 0; i = sql.indexOf(type, i + 1)) {
            count++;
        }
        return count;
    }

    private void insert(int id, String reference, String payload) {
        try (SqlRequest request = sqlConnection.getCreateBuilder(DummyNativeTypes.TYPE)
                .set(DummyNativeTypes.ID, id)
                .set(DummyNativeTypes.REFERENCE, reference)
                .set(DummyNativeTypes.PAYLOAD, payload)
                .set(DummyNativeTypes.VERBATIM, "keep me")
                .set(DummyNativeTypes.NESTED, DummyObject2.TYPE.instantiate().set(DummyObject2.ID, 7L))
                .getRequest()) {
            request.apply();
        }
        sqlConnection.commit();
    }

    private Glob read(int id) {
        return sqlConnection.getQueryBuilder(DummyNativeTypes.TYPE,
                        Constraints.equal(DummyNativeTypes.ID, id))
                .selectAll().getQuery().executeUnique();
    }
}
