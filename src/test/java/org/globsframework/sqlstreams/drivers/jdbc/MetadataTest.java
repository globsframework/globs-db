package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.StringArrayField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.metamodel.type.DataType;
import org.globsframework.sqlstreams.model.DummyObject;
import org.junit.Test;

import static junit.framework.Assert.*;

public class MetadataTest extends DbServicesTestCase {

    @Test
    public void testSimple() throws Exception {
        JdbcConnection db = sqlService.getDb();
        GlobType type = db.extractType(sqlService.getTableName(DummyObject.TYPE))
                .forceType(sqlService.getColumnName(DummyObject.DATE), DataType.Integer)
                .forceType(sqlService.getColumnName(DummyObject.CREATED_AT), DataType.Long)
                .extract();
        assertEquals(DummyObject.TYPE.getFieldCount(), type.getFieldCount());
        for (Field field : DummyObject.TYPE.getFields()) {
            Field actualField = type.findField(sqlService.getColumnName(field));
            assertNotNull(field.getName(), actualField);
            if (field instanceof StringArrayField) {
                assertTrue(actualField.getClass().getName() + " != " + StringField.class.getName(), StringField.class.isAssignableFrom(actualField.getClass()));
            } else {
                assertTrue(actualField.getClass().getName() + " != " + field.getClass().getName(), actualField.getClass().isInstance(field));
            }
        }
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
