package org.globsframework.sqlstreams.accessors;

import org.globsframework.streams.accessors.LongAccessor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.Date;

public class DateLongSqlAccessor extends SqlAccessor implements LongAccessor {

    public Long getLong() {
        Date date = getSqlMoStream().getDate(getIndex());
        if (date == null) {
            return null;
        }
        LocalDate ldt = LocalDate.of(date.getYear() + 1900, date.getMonth() + 1, date.getDate());
        return ldt.getLong(ChronoField.EPOCH_DAY);
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
