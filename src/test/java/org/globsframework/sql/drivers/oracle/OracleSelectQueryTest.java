package org.globsframework.sql.drivers.oracle;

import org.globsframework.core.metamodel.GlobModel;
import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.Utils;
import org.globsframework.core.xml.XmlGlobStreamReader;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.drivers.jdbc.DbServicesTestCase;
import org.globsframework.sql.drivers.jdbc.JdbcConnection;
import org.globsframework.sql.drivers.oracle.request.OracleSqlQueryBuilder;
import org.globsframework.sql.model.DummyObject;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Oracle has no LIMIT/OFFSET. The query is built with the oracle builder but run on the hsqldb test
 * database : both understand the standard "OFFSET n ROWS FETCH NEXT m ROWS ONLY" clause, so the
 * generated request can be checked and executed without an oracle instance.
 */
public class OracleSelectQueryTest extends DbServicesTestCase {

    @Test
    public void topAndSkipUseFetchNextInsteadOfLimit() {
        populate(sqlConnection,
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true'/>" +
                                "<dummyObject id='3' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='4' name='world' value='2.2' present='false'/>" +
                                "<dummyObject id='5' name='world' value='2.2' present='false'/>", directory.get(GlobModel.class)));
        Integer[] values = {1, 2, 3, 4, 5};

        SelectQuery topQuery = oracleQueryBuilder(values)
                .top(1)
                .getQuery();
        assertTrue(topQuery.toString(), topQuery.toString().endsWith(" FETCH NEXT 1 ROWS ONLY"));
        List<Glob> list = topQuery.executeAsGlobs();
        assertEquals(1, list.size());
        assertEquals(5, list.get(0).get(DummyObject.ID).intValue());

        SelectQuery skipQuery = oracleQueryBuilder(values)
                .top(1)
                .skip(1)
                .getQuery();
        assertTrue(skipQuery.toString(), skipQuery.toString().endsWith(" OFFSET 1 ROWS FETCH NEXT 1 ROWS ONLY"));
        List<Glob> skipList = skipQuery.executeAsGlobs();
        assertEquals(1, skipList.size());
        assertEquals(4, skipList.get(0).get(DummyObject.ID).intValue());

        SelectQuery onlySkipQuery = oracleQueryBuilder(values)
                .skip(2)
                .getQuery();
        assertTrue(onlySkipQuery.toString(), onlySkipQuery.toString().endsWith(" OFFSET 2 ROWS"));
        List<Glob> onlySkipList = onlySkipQuery.executeAsGlobs();
        assertEquals(2, onlySkipList.size());
        assertEquals(3, onlySkipList.get(0).get(DummyObject.ID).intValue());
    }

    private OracleSqlQueryBuilder oracleQueryBuilder(Integer[] values) {
        OracleSqlQueryBuilder builder = new OracleSqlQueryBuilder(((JdbcConnection) sqlConnection).getConnection(),
                DummyObject.TYPE, Constraints.in(DummyObject.ID, Utils.set(values)), sqlService);
        builder.withKeys();
        builder.orderDesc(DummyObject.ID);
        return builder;
    }
}
