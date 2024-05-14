package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.metamodel.GlobModel;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.SelectBuilder;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.model.DummyObject;
import org.globsframework.sqlstreams.model.DummyObject2;
import org.globsframework.xml.XmlGlobStreamReader;
import org.junit.Assert;
import org.junit.Test;

public class SqlSelectFromStringTest extends DbServicesTestCase {

    @Test
    public void name() {
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true'/>" +
                        "<dummyObject id='2' name='all' value='2.2' present='false'/>", directory.get(GlobModel.class)));
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject2 id='2' label='world'/>", directory.get(GlobModel.class)));
        final String sql = "select " + rename(DummyObject.ID, "ID") + ", " + rename(DummyObject.NAME, "NAME") + ", " + rename(DummyObject2.LABEL, "LABEL")
                           + " from " + sqlService.getTableName(DummyObject.TYPE) + ", " + sqlService.getTableName(DummyObject2.TYPE) + " where " + sqlService.getTableName(DummyObject.TYPE) + "." +
                           sqlService.getColumnName(DummyObject.ID) + " = " + sqlService.getTableName(DummyObject2.TYPE) + "." +
                           sqlService.getColumnName(DummyObject2.ID);
        final GlobType globType = sqlConnection.extractFromQuery(sql + " AND  0=1");
        System.out.println(globType.describe());
        final SelectBuilder queryBuilder = sqlConnection.getQueryBuilder(globType);
        queryBuilder.selectAll();
        final SelectQuery query = queryBuilder.getQuery(sql);
        final GlobList globs = query.executeAsGlobs();
        Assert.assertEquals(1, globs.size());
        final Glob actual = globs.get(0);
        Assert.assertEquals("world", actual.get(actual.getType().getField("LABEL").asStringField()));
        Assert.assertEquals("all", actual.get(actual.getType().getField("NAME").asStringField()));
    }

    private String rename(Field field, String as) {
        return sqlService.getTableName(field.getGlobType()) + "." + sqlService.getColumnName(field) + " as " + as;
    }
}
