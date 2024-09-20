package org.globsframework.sql.accessors;

import org.globsframework.core.streams.accessors.DateAccessor;

import java.time.LocalDate;
import java.util.Date;

public class DateSqlAccessor extends SqlAccessor implements DateAccessor {


    public LocalDate getDate() {
        Date date = getSqlMoStream().getDate(getIndex());
        if (date == null) {
            return null;
        }
        return LocalDate.of(date.getYear() + 1900, date.getMonth() + 1, date.getDate());
    }

    public boolean wasNull() {
        return getDate() == null;
    }

    public Object getObjectValue() {
        return getDate();
    }
}
