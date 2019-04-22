package org.globsframework.sqlstreams.accessors;

import org.globsframework.streams.accessors.LongAccessor;

import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.Date;

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
