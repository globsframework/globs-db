package org.globsframework.sql.accessors;

import org.globsframework.core.streams.accessors.LongAccessor;

import java.sql.Timestamp;

public class DateTimeLongSqlAccessor extends SqlAccessor implements LongAccessor {

    public Long getLong() {
        Timestamp date = getSqlMoStream().getTimeStamp(getIndex());
        if (date == null) {
            return null;
        }
        return date.getTime();
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
