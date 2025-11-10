package org.globsframework.sql.accessors;

import org.globsframework.core.streams.accessors.BytesAccessor;

public class BytesSqlAccessor extends SqlAccessor implements BytesAccessor {
    public Object getObjectValue() {
        return getValue();
    }

    public byte[] getValue() {
        return getSqlMoStream().getBytes(getIndex());
    }
}
