package org.globsframework.sql.drivers.jdbc.request;

import org.globsframework.core.metamodel.GlobModel;
import org.globsframework.core.xml.XmlGlobStreamReader;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.drivers.jdbc.DbServicesTestCase;
import org.globsframework.sql.drivers.jdbc.JdbcConnection;
import org.globsframework.sql.drivers.jdbc.SelectQuerySpec;
import org.globsframework.sql.model.DummyObject;
import org.junit.Test;

import java.time.Duration;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

/**
 * The builder used to hand twelve loose arguments to the query class, duplicated in each dialect.
 * These check that the spec carries what the builder was given, including the two settings that
 * motivated the change.
 */
public class SelectQuerySpecTest extends DbServicesTestCase {

    @Test
    public void theBuilderPassesItsSettingsOn() {
        SqlQueryBuilder builder = builder();
        builder.top(10).skip(2).fetchSize(500).queryTimeout(Duration.ofSeconds(7));

        SelectQuerySpec spec = builder.spec();

        assertEquals(10, spec.top());
        assertEquals(2, spec.skip());
        assertEquals(500, spec.fetchSize());
        assertEquals(Duration.ofSeconds(7), spec.queryTimeout());
        assertEquals(DummyObject.TYPE, spec.fallBackType());
    }

    @Test
    public void withoutSettingsTheDriverDefaultsApply() {
        SelectQuerySpec spec = builder().spec();

        assertEquals(0, spec.fetchSize());
        assertNull(spec.queryTimeout());
    }

    @Test
    public void theServiceDefaultsReachEveryQuery() {
        sqlService.setDefaultFetchSize(250);
        sqlService.setDefaultQueryTimeout(Duration.ofSeconds(30));

        SelectQuerySpec spec = builder().spec();

        assertEquals(250, spec.fetchSize());
        assertEquals(Duration.ofSeconds(30), spec.queryTimeout());
    }

    @Test
    public void aQuerySetsItsOwnFetchSizeOverTheServiceDefault() {
        sqlService.setDefaultFetchSize(250);

        SqlQueryBuilder builder = builder();
        builder.fetchSize(10);

        assertEquals(10, builder.spec().fetchSize());
    }

    @Test
    public void aFetchSizeDoesNotChangeWhatIsRead() {
        populate(sqlConnection, XmlGlobStreamReader.parse(
                "<dummyObject id='1' name='a'/><dummyObject id='2' name='b'/><dummyObject id='3' name='c'/>",
                directory.get(GlobModel.class)));

        try (SelectQuery query = sqlConnection.getQueryBuilder(DummyObject.TYPE)
                .selectAll()
                .fetchSize(1)
                .queryTimeout(Duration.ofSeconds(30))
                .getQuery()) {
            assertEquals(3, query.executeAsGlobs().size());
        }
    }

    @Test
    public void theSpecCanStillBeBuiltTheOldWay() {
        // the shape the query classes took before fetch size and timeout existed
        SelectQuerySpec spec = new SelectQuerySpec(Constraints.equal(DummyObject.ID, 1),
                java.util.Map.of(), sqlService, true, java.util.List.of(), java.util.List.of(),
                -1, -1, java.util.Set.of(), java.util.List.of(), DummyObject.TYPE);

        assertEquals(0, spec.fetchSize());
        assertNull(spec.queryTimeout());
    }

    private SqlQueryBuilder builder() {
        return new SqlQueryBuilder(((JdbcConnection) sqlConnection).getConnection(), DummyObject.TYPE,
                null, sqlService);
    }
}
