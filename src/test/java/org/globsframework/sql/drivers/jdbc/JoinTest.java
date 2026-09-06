package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobModel;
import org.globsframework.core.model.Glob;
import org.globsframework.core.streams.GlobStream;
import org.globsframework.core.streams.accessors.Accessor;
import org.globsframework.core.xml.XmlGlobStreamReader;
import org.globsframework.sql.ColumnRef;
import org.globsframework.sql.SelectBuilder;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.TableRef;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.exceptions.SqlException;
import org.globsframework.sql.model.DummyObject;
import org.globsframework.sql.model.DummyObject2;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * A query used to name several tables only by accident — a constraint reaching across types added
 * both to the FROM clause, which is a cross join filtered by a WHERE. There was no way to write a
 * LEFT JOIN, no ON clause, and no alias, so a table could not be joined to itself.
 */
public class JoinTest extends DbServicesTestCase {

    @Test
    public void innerJoinKeepsOnlyTheMatchingRows() {
        populateBoth();

        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        Joined refs = joined(qb, false);
        Accessor name = qb.retrieveUnTyped(refs.root().column(DummyObject.NAME));
        Accessor label = qb.retrieveUnTyped(refs.other().column(DummyObject2.LABEL));
        qb.orderAsc(DummyObject.ID);

        assertEquals(List.of("first/un", "second/deux"), read(qb, name, label));
    }

    @Test
    public void leftJoinKeepsTheRowsWithoutAMatch() {
        populateBoth();

        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        Joined refs = joined(qb, true);
        Accessor name = qb.retrieveUnTyped(refs.root().column(DummyObject.NAME));
        Accessor label = qb.retrieveUnTyped(refs.other().column(DummyObject2.LABEL));
        qb.orderAsc(DummyObject.ID);

        // the row with no counterpart is kept, and gets null for the joined columns
        assertEquals(List.of("first/un", "second/deux", "orphan/null"), read(qb, name, label));
    }

    @Test
    public void aGlobCarriesTheRootTableWhileAccessorsCarryTheJoinedOne() {
        populateBoth();

        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        Joined refs = joined(qb, false);
        // a glob has one type: the joined table's columns travel through an accessor, not through it
        qb.select(refs.root().column(DummyObject.NAME)).select(refs.root().column(DummyObject.ID));
        qb.orderAsc(DummyObject.ID);

        List<Glob> rows = qb.getQuery().executeAsGlobs();

        assertEquals(2, rows.size());
        assertEquals(List.of("first", "second"), names(rows));
    }

    @Test
    public void aTableCanBeJoinedToItself() {
        populateSelfReferencing();

        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        TableRef child = qb.rootTable();
        TableRef parent = qb.table(DummyObject.TYPE);
        qb.innerJoin(parent, Constraints.equal(child.column(DummyObject.LINK_ID),
                parent.column(DummyObject.ID)));
        // both sides carry the same field, and a glob holds one value per field, so at least one of
        // them is read through its own accessor
        Accessor childName = qb.retrieveUnTyped(child.column(DummyObject.NAME));
        Accessor parentName = qb.retrieveUnTyped(parent.column(DummyObject.NAME));
        // a bare field would be ambiguous here: both sides carry it
        qb.orderAsc(child.column(DummyObject.ID));

        List<String> pairs = new ArrayList<>();
        GlobStream stream = qb.getQuery().execute();
        while (stream.next()) {
            pairs.add(childName.getObjectValue() + " -> " + parentName.getObjectValue());
        }

        assertEquals(List.of("child -> root", "grandchild -> child"), pairs);
    }

    @Test
    public void aBareFieldIsRefusedWhenItsTypeIsJoinedTwice() {
        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        TableRef child = qb.rootTable();
        TableRef parent = qb.table(DummyObject.TYPE);
        qb.innerJoin(parent, Constraints.equal(child.column(DummyObject.LINK_ID),
                parent.column(DummyObject.ID)));
        qb.retrieveUnTyped(child.column(DummyObject.NAME));
        qb.orderAsc(DummyObject.ID);

        SqlException thrown = assertThrows(SqlException.class, qb::getQuery);

        assertTrue(thrown.getMessage(), thrown.getMessage().contains("ambiguous"));
        assertTrue(thrown.getMessage(), thrown.getMessage().contains("TableRef.column"));
    }

    @Test
    public void aFieldOfATableTheQueryDoesNotJoinIsRefused() {
        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        TableRef root = qb.rootTable();
        TableRef other = qb.table(DummyObject2.TYPE);
        qb.innerJoin(other, Constraints.equal(root.column(DummyObject.LINK2_ID), other.column(DummyObject2.ID)));
        qb.select(DummyObject.NAME);
        qb.orderAsc(DummyObject.ID);

        // the query is fine as long as every field belongs to a joined table
        assertNotNull(qb.getQuery());
    }

    @Test
    public void theGeneratedSqlNamesEachOccurrence() {
        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        TableRef root = qb.rootTable();
        TableRef other = qb.table(DummyObject2.TYPE);
        qb.leftJoin(other, Constraints.equal(root.column(DummyObject.LINK2_ID), other.column(DummyObject2.ID)));
        qb.select(root.column(DummyObject.NAME)).orderAsc(DummyObject.ID);

        String sql;
        try (SelectQuery query = qb.getQuery()) {
            sql = query.toString();
        }

        assertTrue(sql, sql.contains(" LEFT JOIN "));
        assertTrue(sql, sql.contains(" ON ("));
        assertTrue(sql, sql.contains("t0."));
        assertTrue(sql, sql.contains("t1."));
        // no AS before a table alias: Oracle only accepts it in front of a column alias
        assertFalse(sql, sql.contains(" AS t0"));
        // ORDER BY has to say which table it means once there is more than one
        assertTrue(sql, sql.matches(".* ORDER BY t0\\..* ASC"));
    }

    @Test
    public void aQueryWithoutJoinsIsWrittenExactlyAsBefore() {
        SelectQuery query = sqlConnection.getQueryBuilder(DummyObject.TYPE,
                        Constraints.equal(DummyObject.ID, 1))
                .selectAll().getQuery();

        // no alias is introduced for a query that does not join
        assertFalse(query.toString(), query.toString().contains(" t0"));
        query.close();
    }

    @Test
    public void valuesInTheJoinConditionBindBeforeThoseInTheWhere() {
        populateBoth();

        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE,
                Constraints.equal(DummyObject.NAME, "second"));
        TableRef root = qb.rootTable();
        TableRef other = qb.table(DummyObject2.TYPE);
        qb.innerJoin(other, Constraints.and(
                Constraints.equal(root.column(DummyObject.LINK2_ID), other.column(DummyObject2.ID)),
                Constraints.equalsObject(other.column(DummyObject2.LABEL), "deux")));
        Accessor name = qb.retrieveUnTyped(root.column(DummyObject.NAME));
        Accessor label = qb.retrieveUnTyped(other.column(DummyObject2.LABEL));

        // "deux" binds inside the ON clause, "second" in the WHERE that follows it: getting the
        // order wrong swaps the two values and the query returns nothing
        assertEquals(List.of("second/deux"), read(qb, name, label));
    }

    @Test
    public void theJoinItselfFiltersTheRows() {
        populateBoth();

        int all = sqlConnection.getQueryBuilder(DummyObject.TYPE).selectAll().getQuery()
                .executeAsGlobs().size();

        SelectBuilder qb = sqlConnection.getQueryBuilder(DummyObject.TYPE);
        Joined refs = joined(qb, false);
        Accessor name = qb.retrieveUnTyped(refs.root().column(DummyObject.NAME));
        Accessor label = qb.retrieveUnTyped(refs.other().column(DummyObject2.LABEL));

        assertEquals(3, all);
        assertEquals(2, read(qb, name, label).size());
    }

    private record Joined(TableRef root, TableRef other) {
    }

    private Joined joined(SelectBuilder qb, boolean left) {
        TableRef root = qb.rootTable();
        TableRef other = qb.table(DummyObject2.TYPE);
        ColumnRef on = root.column(DummyObject.LINK2_ID);
        if (left) {
            qb.leftJoin(other, Constraints.equal(on, other.column(DummyObject2.ID)));
        } else {
            qb.innerJoin(other, Constraints.equal(on, other.column(DummyObject2.ID)));
        }
        return new Joined(root, other);
    }

    private List<String> read(SelectBuilder qb, Accessor left, Accessor right) {
        List<String> rows = new ArrayList<>();
        GlobStream stream = qb.getQuery().execute();
        while (stream.next()) {
            rows.add(left.getObjectValue() + "/" + right.getObjectValue());
        }
        return rows;
    }

    private List<String> names(List<Glob> rows) {
        return rows.stream().map(glob -> glob.get(DummyObject.NAME)).toList();
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

    private void populateSelfReferencing() {
        populate(sqlConnection, XmlGlobStreamReader.parse(
                "<dummyObject id='1' name='root'/>" +
                        "<dummyObject id='2' name='child' linkId='1'/>" +
                        "<dummyObject id='3' name='grandchild' linkId='2'/>",
                directory.get(GlobModel.class)));
    }
}
