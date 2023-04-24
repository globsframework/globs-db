package org.globsframework.sqlstreams.constraints.impl;

import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.ConstraintVisitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class AndConstraint extends ArrayConstraint implements Constraint {

    public AndConstraint(Constraint[] constraints) {
        super(constraints);
    }

    public static Constraint build(Constraint l, Constraint r) {
        if (l == null) {
            return r;
        }
        if (r == null) {
            return l;
        }
        return new AndConstraint(new Constraint[]{l, r});
    }

    public static Constraint build(Constraint[] constraints) {
        if (constraints.length == 1) {
            return constraints[0];
        }
        if (constraints.length == 2) {
            return build(constraints[0], constraints[1]);
        }
        List<Constraint> c = new ArrayList<>();
        for (Constraint constraint : constraints) {
            if (constraint != null) {
                if (constraint instanceof AndConstraint) {
                    c.addAll(Arrays.asList(((AndConstraint) constraint).getConstraints()));
                }
                else {
                    c.add(constraint);
                }
            }
        }
        return new AndConstraint(c.toArray(Constraint[]::new));
    }

    public <T extends ConstraintVisitor> T visit(T visitor) {
        visitor.visitAnd(this);
        return visitor;
    }
}
