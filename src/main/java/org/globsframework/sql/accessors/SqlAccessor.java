package org.globsframework.sql.accessors;

import org.globsframework.core.streams.accessors.Accessor;
import org.globsframework.sql.drivers.jdbc.SqlDbStream;

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
