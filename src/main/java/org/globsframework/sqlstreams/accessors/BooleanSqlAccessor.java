package org.globsframework.sqlstreams.accessors;

import org.globsframework.sqlstreams.drivers.jdbc.SqlGlobStream;
import org.globsframework.streams.accessors.BooleanAccessor;

public class BooleanSqlAccessor extends SqlAccessor implements BooleanAccessor {
    private Boolean cachedValue;
    private boolean isNull;
    private int rowId;

    public Boolean getBoolean() {
        SqlGlobStream moStream = getSqlMoStream();
        if (moStream.getCurrentRowId() == rowId) {
            return cachedValue;
        } else {
            cachedValue = moStream.getBoolean(getIndex());
            rowId = moStream.getCurrentRowId();
            isNull = getSqlMoStream().isNull();
            if (isNull) {
                cachedValue = null;
            }
            return cachedValue;
        }
    }

    public boolean getValue(boolean valueIfNull) {
        Boolean value = getBoolean();
        if (value == null) {
            return valueIfNull;
        }
        return value;
    }

    public Object getObjectValue() {
        return getBoolean();
    }
}
