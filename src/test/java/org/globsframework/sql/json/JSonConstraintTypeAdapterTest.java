package org.globsframework.sql.json;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.globsframework.core.utils.Utils;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.constraints.impl.*;
import org.globsframework.sql.model.DummyObject;
import org.junit.Assert;
import org.junit.Test;

import java.io.StringReader;

public class JSonConstraintTypeAdapterTest {


    @Test
    public void write() {
        Constraint constraint = Constraints.or(Constraints.and(Constraints.equal(DummyObject.NAME, "a name"),
                        Constraints.equal(DummyObject.ID, 3)),
                Constraints.and(Constraints.in(DummyObject.VALUE, Utils.set(1.1, 2.2)),
                        Constraints.contains(DummyObject.NAME, "m")));
        Gson gson = JSonConstraintTypeAdapter.create(name -> DummyObject.TYPE);
        String s = gson.toJson(constraint);
        assertEquivalent("{\n" +
                "  \"or\": [\n" +
                "    {\n" +
                "      \"and\": [\n" +
                "        {\n" +
                "          \"equal\": {\n" +
                "            \"left\": {\n" +
                "              \"field\": {\n" +
                "                \"type\": \"dummyObject\",\n" +
                "                \"name\": \"name\"\n" +
                "              }\n" +
                "            },\n" +
                "            \"right\": {\n" +
                "              \"value\": \"a name\"\n" +
                "            }\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"equal\": {\n" +
                "            \"left\": {\n" +
                "              \"field\": {\n" +
                "                \"type\": \"dummyObject\",\n" +
                "                \"name\": \"id\"\n" +
                "              }\n" +
                "            },\n" +
                "            \"right\": {\n" +
                "              \"value\": 3\n" +
                "            }\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    },\n" +
                "    {\n" +
                "      \"and\": [\n" +
                "        {\n" +
                "          \"in\": {\n" +
                "            \"field\": {\n" +
                "              \"type\": \"dummyObject\",\n" +
                "              \"name\": \"value\"\n" +
                "            },\n" +
                "            \"values\": [\n" +
                "              1.1,\n" +
                "              2.2\n" +
                "            ]\n" +
                "          }\n" +
                "        },\n" +
                "        {\n" +
                "          \"contains\": {\n" +
                "            \"field\": {\n" +
                "              \"type\": \"dummyObject\",\n" +
                "              \"name\": \"name\"\n" +
                "            },\n" +
                "            \"value\": \"m\"\n" +
                "          }\n" +
                "        }\n" +
                "      ]\n" +
                "    }\n" +
                "  ]\n" +
                "}", s);

        Constraint constraint1 = gson.fromJson(s, Constraint.class);
        Assert.assertTrue(constraint1 instanceof OrConstraint);
        final Constraint[] cOr = ((OrConstraint) constraint1).getConstraints();
        Assert.assertTrue(cOr[0] instanceof AndConstraint);
        Assert.assertTrue(cOr[1] instanceof AndConstraint);
        Constraint andConstraint = cOr[1];
        final Constraint[] cAnd = ((AndConstraint) andConstraint).getConstraints();
        Constraint inConstraint = cAnd[0];
        Assert.assertTrue(inConstraint instanceof InConstraint);
        Assert.assertEquals(((InConstraint) inConstraint).getField(), DummyObject.VALUE);
        Assert.assertTrue(((InConstraint) inConstraint).getValues().contains(1.1));
        Constraint containsConstraint = cAnd[1];
        Assert.assertTrue(containsConstraint instanceof ContainsConstraint);
    }

    @Test
    public void containsOrNot() {
        Constraint constraint = Constraints.and(Constraints.contains(DummyObject.NAME, "a name"),
                Constraints.notContains(DummyObject.NAME, "aaa"));
        Gson gson = JSonConstraintTypeAdapter.create(name -> DummyObject.TYPE);
        String s = gson.toJson(constraint);
        assertEquivalent("{\"and\":[{\"contains\":{\"field\":{\"type\":\"dummyObject\",\"name\":\"name\"},\"value\":\"a name\"}},{\"notContains\":{\"field\":{\"type\":\"dummyObject\",\"name\":\"name\"},\"value\":\"aaa\"}}]}", s);
        Constraint constraint1 = gson.fromJson(s, Constraint.class);
        Assert.assertTrue(constraint1 instanceof AndConstraint);
        final Constraint[] c = ((AndConstraint) constraint1).getConstraints();
        Assert.assertTrue(c[0] instanceof ContainsConstraint);
        Assert.assertTrue(c[1] instanceof ContainsConstraint);
    }

    @Test
    public void containsRegularExpressionCaseInsensitive() {
        Constraint constraint = Constraints.regularExpressionCaseInsensitive(DummyObject.NAME, "^h.*");
        Gson gson = JSonConstraintTypeAdapter.create(name -> DummyObject.TYPE);
        String s = gson.toJson(constraint);
        assertEquivalent("{\"caseInsensitiveRegex\":{\"field\":{\"type\":\"dummyObject\",\"name\":\"name\"},\"value\":\"^h.*\"}}", s);
        Constraint constraint1 = gson.fromJson(s, Constraint.class);
        Assert.assertTrue(constraint1 instanceof RegularExpressionConstraint);
    }

    @Test
    public void containsNotRegularExpressionCaseInsensitive() {
        Constraint constraint = Constraints.notRegularExpressionCaseInsensitive(DummyObject.NAME, "^h.*");
        Gson gson = JSonConstraintTypeAdapter.create(name -> DummyObject.TYPE);
        String s = gson.toJson(constraint);
        assertEquivalent("{\"notCaseInsensitiveRegex\":{\"field\":{\"type\":\"dummyObject\",\"name\":\"name\"},\"value\":\"^h.*\"}}", s);
        Constraint constraint1 = gson.fromJson(s, Constraint.class);
        Assert.assertTrue(constraint1 instanceof RegularExpressionConstraint);
    }

    @Test
    public void containsRegularExpressionCaseSensitive() {
        Constraint constraint = Constraints.regularExpressionCaseSensitive(DummyObject.NAME, "^h.*");
        Gson gson = JSonConstraintTypeAdapter.create(name -> DummyObject.TYPE);
        String s = gson.toJson(constraint);
        assertEquivalent("{\"caseSensitiveRegex\":{\"field\":{\"type\":\"dummyObject\",\"name\":\"name\"},\"value\":\"^h.*\"}}", s);
        Constraint constraint1 = gson.fromJson(s, Constraint.class);
        Assert.assertTrue(constraint1 instanceof RegularExpressionConstraint);
    }

    @Test
    public void isNull() {
        Constraint constraint = Constraints.isNotNull(DummyObject.NAME);
        Gson gson = JSonConstraintTypeAdapter.create(name -> DummyObject.TYPE);
        String s = gson.toJson(constraint);
        assertEquivalent("{\"isNotNull\":{\"field\":{\"type\":\"dummyObject\",\"name\":\"name\"}}}", s);
        Constraint constraint1 = gson.fromJson(s, Constraint.class);
        Assert.assertTrue(constraint1 instanceof NullOrNotConstraint);
        Assert.assertEquals("name", ((NullOrNotConstraint) constraint1).getField().getName());
    }

    @Test
    public void containsNotRegularExpressionCaseSensitive() {
        Constraint constraint = Constraints.notRegularExpressionCaseSensitive(DummyObject.NAME, "^h.*");
        Gson gson = JSonConstraintTypeAdapter.create(name -> DummyObject.TYPE);
        String s = gson.toJson(constraint);
        assertEquivalent("{\"notCaseSensitiveRegex\":{\"field\":{\"type\":\"dummyObject\",\"name\":\"name\"},\"value\":\"^h.*\"}}", s);
        Constraint constraint1 = gson.fromJson(s, Constraint.class);
        Assert.assertTrue(constraint1 instanceof RegularExpressionConstraint);
    }

    public static void assertEquivalent(String expected, String actual) {
        JsonElement expectedTree = JsonParser.parseReader(new StringReader(expected));
        JsonElement actualTree = JsonParser.parseReader(new StringReader(actual));
        Gson gson = new Gson();
        Assert.assertEquals(gson.toJson(expectedTree), gson.toJson(actualTree));
    }


}
