package org.globsframework.sql.accessors;

import org.globsframework.streams.accessors.IntegerAccessor;

public class IntegerSqlAccessor extends SqlAccessor implements IntegerAccessor {

    public Integer getInteger() {
        return getSqlMoStream().getInteger(getIndex());
    }

    public int getValue(int valueIfNull) {
        Integer value = getInteger();
        return value == null ? valueIfNull : value;
    }

    public boolean wasNull() {
        return getInteger() == null;
    }

    public Object getObjectValue() {
        return getInteger();
    }
}
