package org.globsframework.sql.constraints.impl;

import org.globsframework.sql.constraints.Constraint;

public class ArrayConstraint {
    private Constraint[] constraints;

    public ArrayConstraint(Constraint[] constraints) {
        this.constraints = constraints;
    }

    public Constraint[] getConstraints() {
        return constraints;
    }
}
