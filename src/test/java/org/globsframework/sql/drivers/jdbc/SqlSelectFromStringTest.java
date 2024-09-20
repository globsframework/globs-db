package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobModel;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.model.Glob;
import org.globsframework.core.xml.XmlGlobStreamReader;
import org.globsframework.sql.SelectBuilder;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.model.DummyObject;
import org.globsframework.sql.model.DummyObject2;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

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
                + " from " + sqlService.getTableName(DummyObject.TYPE, true) + ", " + sqlService.getTableName(DummyObject2.TYPE, true) + " where " + sqlService.getTableName(DummyObject.TYPE, true) + "." +
                sqlService.getColumnName(DummyObject.ID, true) + " = " + sqlService.getTableName(DummyObject2.TYPE, true) + "." +
                sqlService.getColumnName(DummyObject2.ID, true);
        final GlobType globType = sqlConnection.extractFromQuery(sql + " AND  0=1");
        System.out.println(globType.describe());
        final SelectBuilder queryBuilder = sqlConnection.getQueryBuilder(globType);
        queryBuilder.selectAll();
        final SelectQuery query = queryBuilder.getQuery(sql);
        final List<Glob> globs = query.executeAsGlobs();
        Assert.assertEquals(1, globs.size());
        final Glob actual = globs.get(0);
        Assert.assertEquals("world", actual.get(actual.getType().getField("LABEL").asStringField()));
        Assert.assertEquals("all", actual.get(actual.getType().getField("NAME").asStringField()));
    }

    private String rename(Field field, String as) {
        return sqlService.getTableName(field.getGlobType(), true) + "." + sqlService.getColumnName(field, true) + " as " + as;
    }
}
