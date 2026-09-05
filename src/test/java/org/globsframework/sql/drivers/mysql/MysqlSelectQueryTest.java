package org.globsframework.sql.drivers.mysql;

import org.globsframework.core.metamodel.GlobModel;
import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.Utils;
import org.globsframework.core.xml.XmlGlobStreamReader;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.drivers.jdbc.DbServicesTestCase;
import org.globsframework.sql.drivers.jdbc.JdbcConnection;
import org.globsframework.sql.drivers.mysql.request.MysqlQueryBuilder;
import org.globsframework.sql.model.DummyObject;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * MySQL rejects an OFFSET with no LIMIT in front of it, so skip() alone used to generate a request
 * no MySQL server would run. That clause is checked on the generated SQL — the LIMIT it has to name
 * is 2^64-1, which the test database cannot execute — while the forms both dialects share are also
 * run against hsqldb.
 */
public class MysqlSelectQueryTest extends DbServicesTestCase {

    @Test
    public void skipWithoutTopStillNamesALimit() {
        SelectQuery skipOnly = mysqlQueryBuilder().skip(2).getQuery();

        assertTrue(skipOnly.toString(), skipOnly.toString().endsWith(" LIMIT 18446744073709551615 OFFSET 2"));
    }

    @Test
    public void topAndSkipUseLimitOffset() {
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true'/>" +
                                "<dummyObject id='3' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='4' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='5' name='world' value='2.2' present='false'/>",
                        directory.get(GlobModel.class)));

        SelectQuery topQuery = mysqlQueryBuilder().top(1).getQuery();
        assertTrue(topQuery.toString(), topQuery.toString().endsWith(" LIMIT 1"));
        List<Glob> top = topQuery.executeAsGlobs();
        assertEquals(1, top.size());
        assertEquals(5, top.get(0).get(DummyObject.ID).intValue());

        SelectQuery skipQuery = mysqlQueryBuilder().top(1).skip(1).getQuery();
        assertTrue(skipQuery.toString(), skipQuery.toString().endsWith(" LIMIT 1 OFFSET 1"));
        List<Glob> skipped = skipQuery.executeAsGlobs();
        assertEquals(1, skipped.size());
        assertEquals(4, skipped.get(0).get(DummyObject.ID).intValue());
    }

    private MysqlQueryBuilder mysqlQueryBuilder() {
        MysqlQueryBuilder builder = new MysqlQueryBuilder(((JdbcConnection) sqlConnection).getConnection(),
                DummyObject.TYPE, Constraints.in(DummyObject.ID, Utils.set(1, 2, 3, 4, 5)), sqlService);
        builder.withKeys();
        builder.orderDesc(DummyObject.ID);
        return builder;
    }
}
