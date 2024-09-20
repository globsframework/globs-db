package org.globsframework.sql.accessors;

import org.globsframework.core.streams.accessors.IntegerAccessor;

import java.time.LocalDate;
import java.time.temporal.ChronoField;
import java.util.Date;

public class DateIntegerSqlAccessor extends SqlAccessor implements IntegerAccessor {

    public Integer getInteger() {
        Date date = getSqlMoStream().getDate(getIndex());
        if (date == null) {
            return null;
        }
        LocalDate ldt = LocalDate.of(date.getYear() + 1900, date.getMonth() + 1, date.getDate());
        return Math.toIntExact(ldt.getLong(ChronoField.EPOCH_DAY));
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
