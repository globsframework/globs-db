package org.globsframework.sqlstreams.drivers.jdbc.request;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeBuilder;
import org.globsframework.metamodel.impl.DefaultGlobTypeBuilder;
import org.globsframework.sqlstreams.GlobTypeExtractor;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcConnection;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcSqlService;
import org.junit.Assert;
import org.junit.Test;

public class SchemaTest {


    @Test
    public void addField() {
        JdbcSqlService sqlService = new JdbcSqlService("jdbc:hsqldb:.", "sa", "");

        GlobTypeBuilder typeBuilderF1 = DefaultGlobTypeBuilder.init("TOTO");
        typeBuilderF1.declareStringField("F1");
        GlobType t1 = typeBuilderF1.get();

        JdbcConnection db = sqlService.getDb();
        db.createTable(t1);

        GlobTypeBuilder typeBuilderF2 = DefaultGlobTypeBuilder.init("TOTO");
        typeBuilderF2.declareStringField("F1");
        Field f2 = typeBuilderF2.declareStringField("F2");
        Field f3 = typeBuilderF2.declareStringField("F3");
        GlobType t2 = typeBuilderF2.get();
        db.addColumn(f2, f3);

        GlobTypeExtractor globTypeExtractor = db.extractType(sqlService.getTableName(t2));
        GlobType inDbType = globTypeExtractor.extract();
        Assert.assertTrue(inDbType.hasField(sqlService.getColumnName(f2)));
    }
}
