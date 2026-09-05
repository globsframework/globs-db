package org.globsframework.sql.drivers.jdbc;

import org.globsframework.sql.drivers.postgresql.ToPostgreCaseNamingMapping;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Which NamingMapping a url resolves to. JdbcSqlService and DataSourceSqlService used to disagree on
 * PostgreSQL — the url path handed out DefaultNamingMapping, which leaves names unquoted for
 * PostgreSQL to fold to lower case, while MappingHelper handed out the case-aware one.
 */
public class NamingMappingTest {

    @Test
    public void postgresqlIsCaseAware() {
        assertTrue(JdbcSqlService.getMapping("jdbc:postgresql://localhost:5432/db", null)
                instanceof ToPostgreCaseNamingMapping);
    }

    @Test
    public void everyDialectAgreesWithMappingHelper() {
        assertEquals(MappingHelper.get(DbType.postgresql).getClass(),
                JdbcSqlService.getMapping("jdbc:postgresql://localhost:5432/db", null).getClass());
        assertEquals(MappingHelper.get(DbType.hsqldb).getClass(),
                JdbcSqlService.getMapping("jdbc:hsqldb:.", null).getClass());
        assertEquals(MappingHelper.get(DbType.mysql).getClass(),
                JdbcSqlService.getMapping("jdbc:mysql://localhost/db", null).getClass());
        assertEquals(MappingHelper.get(DbType.mariadb).getClass(),
                JdbcSqlService.getMapping("jdbc:mariadb://localhost/db", null).getClass());
        assertEquals(MappingHelper.get(DbType.oracle).getClass(),
                JdbcSqlService.getMapping("jdbc:oracle:thin:@localhost:1521/db", null).getClass());
    }

    @Test
    public void anExplicitMappingWins() {
        // the migration path for a PostgreSQL database created before the dialect became case-aware:
        // it used to be silently dropped for every recognised dialect
        NamingMapping explicit = DefaultNamingMapping.INSTANCE;
        assertSame(explicit, JdbcSqlService.getMapping("jdbc:postgresql://localhost:5432/db", explicit));
        assertSame(explicit, JdbcSqlService.getMapping("jdbc:hsqldb:.", explicit));
    }

    @Test
    public void postgresqlQuotesOnlyWhatItWouldOtherwiseFold() {
        NamingMapping mapping = new ToPostgreCaseNamingMapping();

        // mixed case would be folded to lower case unquoted, so it is quoted when written into SQL
        assertEquals("\"dummyObject\"", mapping.getTableName("dummyObject", true));
        assertEquals("\"createdAt\"", mapping.getColumnName("createdAt", true));
        // already lower case: nothing to protect
        assertEquals("name", mapping.getColumnName("name", true));

        // unescaped is the name as the database stores it, which is what JDBC metadata reports and
        // what extractType matches on
        assertEquals("dummyObject", mapping.getTableName("dummyObject", false));
        assertEquals("createdAt", mapping.getColumnName("createdAt", false));
    }
}
