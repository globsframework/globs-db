package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.model.Glob;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.model.DummyObject;
import org.globsframework.sql.model.DummyObject2;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * populate used to build a CreateBuilder and a PreparedStatement per glob. It now groups them by
 * shape — the type together with the columns actually written — and batches each group.
 */
public class PopulateTest extends DbServicesTestCase {

    @Test
    public void populatesSeveralTypesAtOnce() {
        List<Glob> globs = List.of(
                DummyObject.TYPE.instantiate().set(DummyObject.ID, 1).set(DummyObject.NAME, "one"),
                DummyObject2.TYPE.instantiate().set(DummyObject2.ID, 10L).set(DummyObject2.LABEL, "ten"),
                DummyObject.TYPE.instantiate().set(DummyObject.ID, 2).set(DummyObject.NAME, "two"),
                DummyObject2.TYPE.instantiate().set(DummyObject2.ID, 20L).set(DummyObject2.LABEL, "twenty"));

        sqlConnection.populate(globs);
        sqlConnection.commit();

        assertEquals(2, countOf(DummyObject.TYPE));
        assertEquals(2, countOf(DummyObject2.TYPE));
        assertEquals("two", readName(2));
    }

    @Test
    public void mixesRowsThatSetTheAutoIncrementKeyAndRowsThatDoNot() {
        // two shapes of the same type: the generated key is written for one row, left to the
        // database for the other, so they cannot share a prepared statement
        List<Glob> globs = List.of(
                DummyObject.TYPE.instantiate().set(DummyObject.ID, 7).set(DummyObject.NAME, "explicit"),
                DummyObject.TYPE.instantiate().set(DummyObject.NAME, "generated"));

        sqlConnection.populate(globs);
        sqlConnection.commit();

        assertEquals(2, countOf(DummyObject.TYPE));
        assertEquals("explicit", readName(7));
    }

    @Test
    public void unsetColumnsAreWrittenAsNull() {
        sqlConnection.populate(List.of(DummyObject.TYPE.instantiate().set(DummyObject.ID, 3)));
        sqlConnection.commit();

        Glob reloaded = sqlConnection.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, 3))
                .selectAll().getQuery().executeUnique();
        assertNull(reloaded.get(DummyObject.NAME));
    }

    @Test
    public void populatesMoreRowsThanOneBatchHolds() {
        List<Glob> globs = new ArrayList<>();
        for (int i = 0; i < 2500; i++) {
            globs.add(DummyObject.TYPE.instantiate().set(DummyObject.ID, i).set(DummyObject.NAME, "row " + i));
        }

        sqlConnection.populate(globs);
        sqlConnection.commit();

        assertEquals(2500, countOf(DummyObject.TYPE));
        assertEquals("row 2499", readName(2499));
    }

    @Test
    public void populatingNothingIsAllowed() {
        sqlConnection.populate(List.of());
        sqlConnection.commit();

        assertEquals(0, countOf(DummyObject.TYPE));
    }

    private String readName(int id) {
        return sqlConnection.getQueryBuilder(DummyObject.TYPE, Constraints.equal(DummyObject.ID, id))
                .selectAll().getQuery().executeUnique().get(DummyObject.NAME);
    }

    private int countOf(org.globsframework.core.metamodel.GlobType type) {
        return sqlConnection.getQueryBuilder(type).selectAll().getQuery().executeAsGlobs().size();
    }
}
