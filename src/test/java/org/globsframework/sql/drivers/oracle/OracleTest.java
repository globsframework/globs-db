package org.globsframework.sql.drivers.oracle;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.fields.DoubleField;
import org.globsframework.core.metamodel.fields.LongField;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.metamodel.impl.DefaultGlobTypeBuilder;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.MutableGlob;
import org.globsframework.sql.GlobTypeExtractor;
import org.globsframework.sql.SqlConnection;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.annotations.IsTimestamp;
import org.globsframework.sql.drivers.jdbc.JdbcSqlService;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.util.List;

public class OracleTest {

    @Test
    @Ignore
    public void simpleCreateInsertSelectTest() throws IOException {
        String url = System.getProperty("oracle.url", "jdbc:oracle:thin:@//127.0.0.1:1521/FREEPDB1");
        String user = System.getProperty("oracle.user", "system");
        String pwd = System.getProperty("oracle.pwd", "oracle");

        SqlService sqlService = new JdbcSqlService(url, user, pwd);
        SqlConnection db = sqlService.getDb();
        GlobTypeBuilder globTypeBuilder = DefaultGlobTypeBuilder.init("TEST_ORA");
        StringField f1 = globTypeBuilder.declareStringField("f1");
        DoubleField f2 = globTypeBuilder.declareDoubleField("f2");
        LongField f3 = globTypeBuilder.declareLongField("f3", IsTimestamp.UNIQUE);

        GlobType globType = globTypeBuilder.build();

        final long epochMilli = ZonedDateTime.now().toInstant().toEpochMilli();
        MutableGlob data = globType.instantiate()
                .set(f1, "ééé")
                .set(f2, 3.3)
                .set(f3, epochMilli);

        final GlobTypeExtractor globTypeExtractor = db.extractType(sqlService.getTableName(globType, true));
        if (globTypeExtractor.extract() != null) {
            db.emptyTable(globType);
            db.getDeleteRequest(globType);
        }
        db.createTable(globType);
        db.populate(List.of(data));

        final Glob glob = db.getQueryBuilder(globType).selectAll().getQuery().executeUnique();
        Assert.assertEquals("ééé", glob.get(f1));
        Assert.assertEquals(3.3, glob.get(f2), 0.001);
        Assert.assertEquals(epochMilli, glob.get(f3).longValue());
        db.commitAndClose();
    }
}
