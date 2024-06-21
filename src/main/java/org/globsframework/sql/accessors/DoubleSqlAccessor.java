package org.globsframework.sql.accessors;

import org.globsframework.streams.accessors.DoubleAccessor;

public class DoubleSqlAccessor extends SqlAccessor implements DoubleAccessor {

    public Double getDouble() {
        return getSqlMoStream().getDouble(getIndex());
    }

    public double getValue(double valueIfNull) {
        Double value = getDouble();
        return value == null ? valueIfNull : value;
    }

    public boolean wasNull() {
        return getDouble() == null;
    }

    public Object getObjectValue() {
        return getDouble();
    }
}
