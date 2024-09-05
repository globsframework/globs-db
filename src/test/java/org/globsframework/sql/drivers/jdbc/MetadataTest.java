package org.globsframework.sql.drivers.jdbc;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.LongArrayField;
import org.globsframework.metamodel.fields.StringArrayField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.metamodel.type.DataType;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.model.DummyObject;
import org.junit.Test;

import static org.junit.Assert.*;


public class MetadataTest extends DbServicesTestCase {

    @Test
    public void testSimple() throws Exception {
        JdbcConnection db = sqlService.getDb();
        GlobType type = db.extractType(sqlService.getTableName(DummyObject.TYPE, true))
                .forceType(sqlService.getColumnName(DummyObject.DATE, true), DataType.Integer)
                .forceType(sqlService.getColumnName(DummyObject.CREATED_AT, true), DataType.Long)
                .extract();
        assertEquals(DummyObject.TYPE.getFieldCount(), type.getFieldCount());
        for (Field field : DummyObject.TYPE.getFields()) {
            Field actualField = type.findField(sqlService.getColumnName(field, true));
            assertNotNull(field.getName(), actualField);
            if (field instanceof StringArrayField || field instanceof LongArrayField) {
                assertTrue(actualField.getClass().getName() + " != " + StringField.class.getName(), StringField.class.isAssignableFrom(actualField.getClass()));
            } else {
                assertTrue(actualField.getClass().getName() + " != " + field.getClass().getName(), actualField.getClass().isInstance(field));
            }
        }
    }

    @Test
    public void extractGlobTypeFromQuery() {
        JdbcConnection db = sqlService.getDb();
        final SqlService jdbcSqlService = db.getJdbcSqlService();
        final GlobType globType = db.extractFromQuery("select * from " + jdbcSqlService.getTableName(DummyObject.TYPE, true) + " where 0=1");
        assertEquals("fromResult", globType.getName());
        assertEquals(13, globType.getFieldCount());
    }

    //    public void testErrorOnNonExistingTable() throws Exception {
//      SqlConnection db = sqlService.getDb();
//      GlobType globType = db.createFrom("toto", Collections.<String>emptySet());
//      assertNull(globType);
//      assertTrue(db.exist(DummyObject.TYPE));
//    }
//
//    public void testFk() throws Exception {
//        SqlConnection db = sqlService.getDb();
//        db.createTable(DummyObjectWithCompositeKey.TYPE, DummyObjectWithLinks.TYPE);
//        db.addFk(DummyObject.TYPE, DummyObjectWithCompositeKey.TYPE, DummyObjectWithLinks.TYPE);
//        SqlConnection.ForeignKey fk = db.getForeignKey(db.getTableName(DummyObjectWithLinks.TYPE), sqlService.getColumnName(DummyObjectWithLinks.PARENT_ID));
//        assertEquals(db.getTableName(DummyObject.TYPE), fk.table);
//        assertEquals(sqlService.getColumnName(DummyObject.ID), fk.columnName);
//        db.dropTable(DummyObjectWithLinks.TYPE, DummyObject.TYPE, DummyObjectWithCompositeKey.TYPE);
//    }
//
//    public void testJdbcProperties() throws Exception{
//        Properties properties = new Properties();
//        properties.setProperty("user", "other");
//        JdbcSqlService sa = new JdbcSqlService("jdbc:hsqldb:.", "sa", "", properties);
//        try {
//            sa.getDb().commitAndClose();
//            fail("Should fail on unknown user");
//        } catch (UnexpectedApplicationState e) {
//            assertTrue(e.getCause().getMessage().contains("not found: other"));
//        }
//    }
}
