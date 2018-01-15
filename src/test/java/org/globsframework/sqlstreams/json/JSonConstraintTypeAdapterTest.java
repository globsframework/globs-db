package org.globsframework.sqlstreams.json;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import org.globsframework.model.DummyObject;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.globsframework.sqlstreams.constraints.impl.AndConstraint;
import org.globsframework.sqlstreams.constraints.impl.ContainsConstraint;
import org.globsframework.sqlstreams.constraints.impl.InConstraint;
import org.globsframework.sqlstreams.constraints.impl.OrConstraint;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class JSonConstraintTypeAdapterTest {


    @Test
    public void write() {
        Constraint constraint = Constraints.or(Constraints.and(Constraints.equal(DummyObject.NAME, "a name"),
              Constraints.equal(DummyObject.ID, 3)),
              Constraints.and(Constraints.in(DummyObject.VALUE, Arrays.asList(1.1, 2.2)),
                    Constraints.contains(DummyObject.NAME, "m")));
        Gson gson = JSonConstraintTypeAdapter.create(name -> DummyObject.TYPE, DummyObject.TYPE);
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
        Assert.assertTrue(((OrConstraint) constraint1).getLeftConstraint() instanceof AndConstraint);
        Constraint andConstraint = ((OrConstraint) constraint1).getRightConstraint();
        Constraint inConstraint = ((AndConstraint) andConstraint).getLeftConstraint();
        Assert.assertTrue(inConstraint instanceof InConstraint);
        Assert.assertEquals(((InConstraint) inConstraint).getField(), DummyObject.VALUE);
        Assert.assertEquals(((Double) ((InConstraint) inConstraint).getValues().get(0)), 1.1, 0.0001);
        Constraint containsConstraint = ((AndConstraint) andConstraint).getRightConstraint();
        Assert.assertTrue(containsConstraint instanceof ContainsConstraint);
    }

    @Test
    public void containsOrNot() {
        Constraint constraint = Constraints.and(Constraints.contains(DummyObject.NAME, "a name"),
              Constraints.notContains(DummyObject.NAME, "aaa"));
        Gson gson = JSonConstraintTypeAdapter.create(name -> DummyObject.TYPE, DummyObject.TYPE);
        String s = gson.toJson(constraint);
        assertEquivalent("{\"and\":[{\"contains\":{\"field\":{\"type\":\"dummyObject\",\"name\":\"name\"},\"value\":\"a name\"}},{\"notContains\":{\"field\":{\"type\":\"dummyObject\",\"name\":\"name\"},\"value\":\"aaa\"}}]}", s);
        Constraint constraint1 = gson.fromJson(s, Constraint.class);
        Assert.assertTrue(constraint1 instanceof AndConstraint);
        Assert.assertTrue(((AndConstraint) constraint1).getLeftConstraint() instanceof ContainsConstraint);
        Assert.assertTrue(((AndConstraint) constraint1).getRightConstraint() instanceof ContainsConstraint);
    }

    public static void assertEquivalent(String expected, String actual) {
        JsonParser jsonParser = new JsonParser();
        JsonElement expectedTree = jsonParser.parse(expected);
        JsonElement actualTree = jsonParser.parse(actual);
        Gson gson = new Gson();
        Assert.assertEquals(gson.toJson(expectedTree), gson.toJson(actualTree));
    }


}