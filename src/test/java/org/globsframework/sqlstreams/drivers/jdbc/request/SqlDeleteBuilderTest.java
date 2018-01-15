package org.globsframework.sqlstreams.drivers.jdbc.request;

import org.globsframework.metamodel.GlobModel;
import org.globsframework.model.GlobList;
import org.globsframework.model.DummyObject;
import org.globsframework.model.DummyObject2;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.globsframework.sqlstreams.drivers.jdbc.DbServicesTestCase;
import org.globsframework.streams.GlobStream;
import org.globsframework.xml.XmlGlobStreamReader;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SqlDeleteBuilderTest extends DbServicesTestCase {

    @Test
    public void testDelete() throws Exception {
        GlobStream streamToWrite =
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
        GlobList globs = sqlConnection.getQueryBuilder(DummyObject.TYPE)
              .withKeys()
              .getQuery().executeAsGlobs();
        assertEquals(1, globs.size());
        assertEquals(2, globs.get(0).get(DummyObject.ID).intValue());
        assertEquals(1, sqlConnection.getQueryBuilder(DummyObject2.TYPE).withKeys().getQuery().executeAsGlobs().size());
    }
}
