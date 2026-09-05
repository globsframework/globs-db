package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
import org.globsframework.core.metamodel.annotations.KeyField;
import org.globsframework.core.metamodel.fields.IntegerField;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.sql.model.DummyObject;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * The indexes a GlobType declares used to be ignored by createTable: they were declared, carried
 * around in the metamodel, and never reached the database.
 */
public class CreateIndexTest extends DbServicesTestCase {

    @Test
    public void createTableCreatesTheDeclaredIndexes() {
        // DummyObject declares a not-unique index on its date field, and setUp created the table
        assertTrue(indexNamesOf(DummyObject.TYPE).toString(),
                hasIndexFor(DummyObject.TYPE, DummyObject.DATE_INDEX.getName()));
    }

    @Test
    public void indexesAreNamedAfterTheirTableSoTwoTypesCanShareAName() {
        GlobTypeBuilder builder = GlobTypeBuilderFactory.create("indexedObject");
        IntegerField id = builder.declareIntegerField("id", KeyField.ZERO);
        StringField label = builder.declareStringField("label");
        // the same index name DummyObject already uses
        builder.addNotUniqueIndex(DummyObject.DATE_INDEX.getName(), label);
        GlobType type = builder.build();

        sqlConnection.createTable(type);

        assertTrue(indexNamesOf(type).toString(), hasIndexFor(type, DummyObject.DATE_INDEX.getName()));
        assertTrue(hasIndexFor(DummyObject.TYPE, DummyObject.DATE_INDEX.getName()));
    }

    @Test
    public void createIndexesIsIdempotent() {
        int before = indexNamesOf(DummyObject.TYPE).size();

        sqlConnection.createIndexes(DummyObject.TYPE);
        sqlConnection.createIndexes(DummyObject.TYPE);

        assertFalse(indexNamesOf(DummyObject.TYPE).isEmpty());
        assertEquals(before, indexNamesOf(DummyObject.TYPE).size());
    }

    @Test
    public void aTypeWithoutIndexesIsLeftAlone() {
        GlobTypeBuilder builder = GlobTypeBuilderFactory.create("unindexedObject");
        builder.declareIntegerField("id", KeyField.ZERO);
        GlobType type = builder.build();

        sqlConnection.createTable(type);
        sqlConnection.createIndexes(type);
    }

    private boolean hasIndexFor(GlobType type, String declaredName) {
        String expected = sqlService.getColumnName(sqlService.getTableName(type, false) + "_" + declaredName, false);
        return indexNamesOf(type).contains(expected);
    }

    private Set<String> indexNamesOf(GlobType type) {
        Set<String> names = new HashSet<>();
        try {
            Connection jdbc = ((JdbcConnection) sqlConnection).getConnection();
            try (ResultSet indexInfo = jdbc.getMetaData().getIndexInfo(jdbc.getCatalog(), null,
                    sqlService.getTableName(type, false), false, true)) {
                while (indexInfo.next()) {
                    String name = indexInfo.getString("INDEX_NAME");
                    if (name != null) {
                        names.add(name);
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return names;
    }
}
