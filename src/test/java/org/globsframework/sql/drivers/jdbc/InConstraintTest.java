package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobModel;
import org.globsframework.core.model.Glob;
import org.globsframework.core.utils.Utils;
import org.globsframework.core.xml.XmlGlobStreamReader;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.drivers.jdbc.impl.InClause;
import org.globsframework.sql.model.DummyObject;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class InConstraintTest extends DbServicesTestCase {

    @Test
    public void notInBindsItsValues() {
        populateFive();

        // NOT IN wrote one placeholder per value and bound none of them, so every notIn constraint
        // reached the database with unbound parameters
        assertEquals(List.of(3, 4, 5), idsMatching(Constraints.notIn(DummyObject.ID, Utils.set(1, 2))));
    }

    @Test
    public void inSelectsTheListedRows() {
        populateFive();

        assertEquals(List.of(1, 4), idsMatching(Constraints.in(DummyObject.ID, Utils.set(1, 4, 42))));
    }

    @Test
    public void anEmptyInMatchesNothingAndAnEmptyNotInMatchesEverything() {
        populateFive();

        // "x IN ()" is not valid SQL: this used to be a syntax error, not an empty result
        assertEquals(List.of(), idsMatching(Constraints.in(DummyObject.ID, Set.of())));
        assertEquals(List.of(1, 2, 3, 4, 5), idsMatching(Constraints.notIn(DummyObject.ID, Set.of())));
    }

    @Test
    public void setsOfDifferentSizesShareOneStatement() {
        Set<Integer> nine = IntStream.rangeClosed(1, 9).boxed().collect(Collectors.toSet());
        Set<Integer> fifteen = IntStream.rangeClosed(1, 15).boxed().collect(Collectors.toSet());

        String nineSql = sqlOf(Constraints.in(DummyObject.ID, nine));
        String fifteenSql = sqlOf(Constraints.in(DummyObject.ID, fifteen));

        assertEquals(nineSql, fifteenSql);
        assertEquals(16, nineSql.chars().filter(c -> c == '?').count());
    }

    @Test
    public void paddingDoesNotChangeTheResult() {
        populateFive();

        // 9 values rounds up to 16 placeholders; the padding repeats a value already in the set,
        // which is neutral for IN and for NOT IN alike
        Set<Integer> nine = IntStream.rangeClosed(1, 9).boxed().collect(Collectors.toSet());
        assertEquals(List.of(1, 2, 3, 4, 5), idsMatching(Constraints.in(DummyObject.ID, nine)));
        assertEquals(List.of(), idsMatching(Constraints.notIn(DummyObject.ID, nine)));
    }

    @Test
    public void smallSetsAreNotPadded() {
        String sql = sqlOf(Constraints.in(DummyObject.ID, Utils.set(1, 2, 3)));

        assertEquals(3, sql.chars().filter(c -> c == '?').count());
    }

    @Test
    public void placeholderCountRoundsUpAboveEight() {
        assertEquals(0, InClause.placeholderCount(0));
        assertEquals(8, InClause.placeholderCount(8));
        assertEquals(16, InClause.placeholderCount(9));
        assertEquals(16, InClause.placeholderCount(16));
        assertEquals(32, InClause.placeholderCount(17));
        assertEquals(1024, InClause.placeholderCount(1024));
        assertEquals(2048, InClause.placeholderCount(1025));
        assertEquals(3072, InClause.placeholderCount(2049));
        assertTrue(InClause.placeholderCount(5000) >= 5000);
    }

    private void populateFive() {
        populate(sqlConnection, XmlGlobStreamReader.parse(
                "<dummyObject id='1' name='a'/><dummyObject id='2' name='b'/><dummyObject id='3' name='c'/>" +
                        "<dummyObject id='4' name='d'/><dummyObject id='5' name='e'/>",
                directory.get(GlobModel.class)));
    }

    private List<Integer> idsMatching(Constraint constraint) {
        return sqlConnection.getQueryBuilder(DummyObject.TYPE, constraint)
                .selectAll().orderAsc(DummyObject.ID).getQuery().executeAsGlobs()
                .stream().map(glob -> glob.get(DummyObject.ID)).toList();
    }

    private String sqlOf(Constraint constraint) {
        try (SelectQuery query = sqlConnection.getQueryBuilder(DummyObject.TYPE, constraint)
                .selectAll().getQuery()) {
            return query.toString();
        }
    }
}
