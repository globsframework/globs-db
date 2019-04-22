package org.globsframework.sqlstreams.accessors;

import org.globsframework.sqlstreams.drivers.jdbc.SqlDbStream;
import org.globsframework.streams.accessors.Accessor;

public abstract class SqlAccessor implements Accessor {
    private SqlDbStream sqlMoStream;
    private int index;

    public void setMoStream(SqlDbStream sqlMoStream) {
        this.sqlMoStream = sqlMoStream;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public SqlDbStream getSqlMoStream() {
        return sqlMoStream;
    }

}
