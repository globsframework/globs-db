package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobModel;
import org.globsframework.core.model.Glob;
import org.globsframework.core.xml.XmlGlobStreamReader;
import org.globsframework.sql.SelectBuilder;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.TableRef;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.model.DummyObject;
import org.globsframework.sql.model.DummyObject2;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * not, between, exists and the in that takes a query: the four the constraint tree could not
 * express. A negated and/or had to be pushed through De Morgan by hand, a range written as two
 * comparisons, and anything needing a nested SELECT was out of reach entirely.
 */
public class MissingConstraintsTest extends DbServicesTestCase {

    @Test
    public void notNegatesAWholeSubtree() {
        populateFive();

        Constraint firstOrLast = Constraints.or(Constraints.equal(DummyObject.ID, 1),
                Constraints.equal(DummyObject.ID, 5));

        assertEquals(List.of(1, 5), idsMatching(firstOrLast));
        assertEquals(List.of(2, 3, 4), idsMatching(Constraints.not(firstOrLast)));
    }

    @Test
    public void notCanBeNested() {
        populateFive();

        assertEquals(List.of(1, 2, 3, 4, 5),
                idsMatching(Constraints.not(Constraints.not(Constraints.greater(DummyObject.ID, 0)))));
        assertEquals(List.of(),
                idsMatching(Constraints.not(Constraints.greater(DummyObject.ID, 0))));
    }

    @Test
    public void betweenIncludesItsBounds() {
        populateFive();

        assertEquals(List.of(2, 3, 4), idsMatching(Constraints.between(DummyObject.ID, 2, 4)));
        assertEquals(List.of(3), idsMatching(Constraints.between(DummyObject.ID, 3, 3)));
        assertEquals(List.of(), idsMatching(Constraints.between(DummyObject.ID, 4, 2)));
    }

    @Test
    public void betweenIsWrittenAsBetween() {
        try (SelectQuery query = sqlConnection.getQueryBuilder(DummyObject.TYPE,
                Constraints.between(DummyObject.ID, 2, 4)).selectAll().getQuery()) {
            String sql = query.toString();
            assertTrue(sql, sql.contains(" BETWEEN "));
            assertTrue(sql, sql.contains(" AND "));
            assertEquals(sql, 2, sql.chars().filter(c -> c == '?').count());
        }
    }

    @Test
    public void betweenAndNotCombine() {
        populateFive();

        assertEquals(List.of(1, 5),
                idsMatching(Constraints.not(Constraints.between(DummyObject.ID, 2, 4))));
    }

    @Test
    public void existsKeepsTheRowsWithACounterpart() {
        populateBoth();

        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        TableRef root = qb.rootTable();
        TableRef other = qb.table(DummyObject2.TYPE);
        qb.where(Constraints.exists(other, Constraints.equal(root.column(DummyObject.LINK2_ID),
                other.column(DummyObject2.ID))));
        qb.selectAll().orderAsc(DummyObject.ID);

        assertEquals(List.of(1, 2), ids(qb.getQuery().executeAsGlobs()));
    }

    @Test
    public void notExistsKeepsTheRowsWithout() {
        populateBoth();

        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        TableRef root = qb.rootTable();
        TableRef other = qb.table(DummyObject2.TYPE);
        qb.where(Constraints.notExists(other, Constraints.equal(root.column(DummyObject.LINK2_ID),
                other.column(DummyObject2.ID))));
        qb.selectAll().orderAsc(DummyObject.ID);

        assertEquals(List.of(3), ids(qb.getQuery().executeAsGlobs()));
    }

    @Test
    public void aSubQueryCanCarryItsOwnValues() {
        populateBoth();

        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        TableRef root = qb.rootTable();
        TableRef other = qb.table(DummyObject2.TYPE);
        // the value inside the subquery binds before the one in the enclosing WHERE
        qb.where(Constraints.exists(other, Constraints.and(
                Constraints.equal(root.column(DummyObject.LINK2_ID), other.column(DummyObject2.ID)),
                Constraints.equalsObject(other.column(DummyObject2.LABEL), "deux"))));
        qb.where(Constraints.equal(DummyObject.NAME, "second"));
        qb.selectAll();

        assertEquals(List.of(2), ids(qb.getQuery().executeAsGlobs()));
    }

    @Test
    public void inTakesAQueryInsteadOfASetOfValues() {
        populateBoth();

        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        qb.rootTable();
        TableRef other = qb.table(DummyObject2.TYPE);
        qb.where(Constraints.in(DummyObject.LINK2_ID, other.column(DummyObject2.ID),
                Constraints.equalsObject(other.column(DummyObject2.LABEL), "un")));
        qb.selectAll().orderAsc(DummyObject.ID);

        assertEquals(List.of(1), ids(qb.getQuery().executeAsGlobs()));
    }

    @Test
    public void notInTakesAQueryToo() {
        populateBoth();

        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        qb.rootTable();
        TableRef other = qb.table(DummyObject2.TYPE);
        qb.where(Constraints.notIn(DummyObject.LINK2_ID, other.column(DummyObject2.ID), null));
        qb.selectAll().orderAsc(DummyObject.ID);

        assertEquals(List.of(3), ids(qb.getQuery().executeAsGlobs()));
    }

    @Test
    public void aSubQueryIsWrittenAsANestedSelect() {
        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        TableRef root = qb.rootTable();
        TableRef other = qb.table(DummyObject2.TYPE);
        qb.where(Constraints.exists(other, Constraints.equal(root.column(DummyObject.LINK2_ID),
                other.column(DummyObject2.ID))));
        qb.selectAll();

        try (SelectQuery query = qb.getQuery()) {
            String sql = query.toString();
            assertTrue(sql, sql.contains(" EXISTS (SELECT 1 FROM "));
            // the subquery has its own alias and correlates on the enclosing one
            assertTrue(sql, sql.contains("t1"));
            assertTrue(sql, sql.contains("t0."));
        }
    }

    private void populateFive() {
        populate(sqlConnection, XmlGlobStreamReader.parse(
                "<dummyObject id='1' name='a'/><dummyObject id='2' name='b'/><dummyObject id='3' name='c'/>" +
                        "<dummyObject id='4' name='d'/><dummyObject id='5' name='e'/>",
                directory.get(GlobModel.class)));
    }

    private void populateBoth() {
        populate(sqlConnection, XmlGlobStreamReader.parse(
                "<dummyObject id='1' name='first' link2Id='10'/>" +
                        "<dummyObject id='2' name='second' link2Id='20'/>" +
                        "<dummyObject id='3' name='orphan' link2Id='99'/>",
                directory.get(GlobModel.class)));
        populate(sqlConnection, XmlGlobStreamReader.parse(
                "<dummyObject2 id='10' label='un'/><dummyObject2 id='20' label='deux'/>",
                directory.get(GlobModel.class)));
    }

    private List<Integer> idsMatching(Constraint constraint) {
        return ids(sqlConnection.getQueryBuilder(DummyObject.TYPE, constraint)
                .selectAll().orderAsc(DummyObject.ID).getQuery().executeAsGlobs());
    }

    private List<Integer> ids(List<Glob> rows) {
        return rows.stream().map(glob -> glob.get(DummyObject.ID)).toList();
    }
}
