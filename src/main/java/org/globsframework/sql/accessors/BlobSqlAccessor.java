package org.globsframework.sql.accessors;

import org.globsframework.core.streams.accessors.BlobAccessor;

public class BlobSqlAccessor extends SqlAccessor implements BlobAccessor {
    public Object getObjectValue() {
        return getValue();
    }

    public byte[] getValue() {
        return getSqlMoStream().getBytes(getIndex());
    }
}
