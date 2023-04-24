package org.globsframework.sqlstreams.constraints.impl;

import org.globsframework.sqlstreams.constraints.Constraint;

public class ArrayConstraint {
    private Constraint[] constraints;

    public ArrayConstraint(Constraint[] constraints) {
        this.constraints = constraints;
    }

    public Constraint[] getConstraints() {
        return constraints;
    }
}
