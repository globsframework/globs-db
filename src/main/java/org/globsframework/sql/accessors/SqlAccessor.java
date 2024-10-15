package org.globsframework.sql.accessors;

import org.globsframework.core.streams.accessors.Accessor;
import org.globsframework.sql.drivers.jdbc.SqlGlobStream;

public abstract class SqlAccessor implements Accessor {
    private SqlGlobStream sqlMoStream;
    private int index;

    public void setMoStream(SqlGlobStream sqlMoStream) {
        this.sqlMoStream = sqlMoStream;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }

    public SqlGlobStream getSqlMoStream() {
        return sqlMoStream;
    }

}
