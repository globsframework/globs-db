package org.globsframework.sql.drivers.jdbc.request;

import org.globsframework.core.metamodel.GlobModel;
import org.globsframework.core.model.Glob;
import org.globsframework.core.streams.DbStream;
import org.globsframework.core.xml.XmlGlobStreamReader;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.drivers.jdbc.DbServicesTestCase;
import org.globsframework.sql.model.DummyObject;
import org.globsframework.sql.model.DummyObject2;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class SqlDeleteBuilderTest extends DbServicesTestCase {

    @Test
    public void testDelete() throws Exception {
        DbStream streamToWrite =
                XmlGlobStreamReader.parse(
                        "<dummyObject id='1' name='hello' value='1.1' present='true'/>", directory.get(GlobModel.class));
        populate(sqlConnection, streamToWrite);
        sqlConnection.getDeleteRequest(DummyObject.TYPE).run();
        assertEquals(0, sqlConnection.getQueryBuilder(DummyObject.TYPE).withKeys().getQuery().executeAsGlobs().size());
    }

    @Test
    public void testDeleteWithConstraint() throws Exception {
        populate(sqlConnection, XmlGlobStreamReader.parse(
                "<dummyObject id='1' name='hello' value='1.1' present='true'/>" +
                        "<dummyObject id='2' name='world' value='1.1' present='true'/>", directory.get(GlobModel.class)));
        populate(sqlConnection, XmlGlobStreamReader.parse(
                "<dummyObject2 id='1' label='hello'/>", directory.get(GlobModel.class)));
        Constraint constraint = Constraints.equal(DummyObject.NAME, "hello");
        sqlConnection.getDeleteRequest(DummyObject.TYPE, constraint).run();
        List<Glob> globs = sqlConnection.getQueryBuilder(DummyObject.TYPE)
                .withKeys()
                .getQuery().executeAsGlobs();
        assertEquals(1, globs.size());
        assertEquals(2, globs.get(0).get(DummyObject.ID).intValue());
        assertEquals(1, sqlConnection.getQueryBuilder(DummyObject2.TYPE).withKeys().getQuery().executeAsGlobs().size());
    }
}
