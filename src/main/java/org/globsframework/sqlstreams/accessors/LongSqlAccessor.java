package org.globsframework.sqlstreams.accessors;

import org.globsframework.streams.accessors.LongAccessor;

public class LongSqlAccessor extends SqlAccessor implements LongAccessor {

    public Long getLong() {
        return getSqlMoStream().getLong(getIndex());
    }

    public long getValue(long valueIfNull) {
        Long value = getLong();
        return value == null ? valueIfNull : value;
    }

    public boolean wasNull() {
        return getLong() == null;
    }

    public Object getObjectValue() {
        return getLong();
    }
}
