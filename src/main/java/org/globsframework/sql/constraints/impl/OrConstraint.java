package org.globsframework.sql.constraints.impl;

import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.ConstraintVisitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class OrConstraint extends ArrayConstraint implements Constraint {
    public OrConstraint(Constraint[] constraints) {
        super(constraints);
    }

    public static Constraint build(Constraint l, Constraint r) {
        if (l == null) {
            return r;
        }
        if (r == null) {
            return l;
        }
        return new OrConstraint(new Constraint[]{l, r});
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
                if (constraint instanceof OrConstraint) {
                    c.addAll(Arrays.asList(((OrConstraint) constraint).getConstraints()));
                }
                else {
                    c.add(constraint);
                }
            }
        }
        if (c.isEmpty()) {
            return null;
        }
        return new OrConstraint(c.toArray(Constraint[]::new));
    }
    public <T extends ConstraintVisitor> T accept(T visitor) {
        visitor.visitOr(this);
        return visitor;
    }
}
