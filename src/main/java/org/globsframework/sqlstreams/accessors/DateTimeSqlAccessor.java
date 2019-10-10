package org.globsframework.sqlstreams.accessors;

import org.globsframework.streams.accessors.DateTimeAccessor;
import org.globsframework.streams.accessors.LongAccessor;

import java.sql.Timestamp;
import java.time.*;

public class DateTimeSqlAccessor extends SqlAccessor implements DateTimeAccessor {

    public ZonedDateTime getDateTime() {
        Timestamp date = getSqlMoStream().getTimeStamp(getIndex());
        if (date == null) {
            return null;
        }
        LocalDateTime localDateTime = date.toLocalDateTime();
        return ZonedDateTime.of(localDateTime, ZoneId.systemDefault());
//        return ZonedDateTime.of(LocalDate.of(date.getYear() + 1900, date.getMonth() + 1, date.getDate()),
//                LocalTime.of(date.getHours(), date.getMinutes(),
//                        date.getSeconds(), date.getNanos()), ZoneId.systemDefault());
    }

    public boolean wasNull() {
        return getDateTime() == null;
    }

    public Object getObjectValue() {
        return getDateTime();
    }
}
