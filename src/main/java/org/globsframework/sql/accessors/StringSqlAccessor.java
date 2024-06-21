package org.globsframework.sql.accessors;

import org.globsframework.streams.accessors.StringAccessor;

public class StringSqlAccessor extends SqlAccessor implements StringAccessor {

    public String getString() {
        return getSqlMoStream().getString(getIndex());
    }

    public Object getObjectValue() {
        return getString();
    }
}
