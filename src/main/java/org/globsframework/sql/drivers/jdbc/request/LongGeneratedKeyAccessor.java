package org.globsframework.sql.drivers.jdbc.request;

import org.globsframework.core.metamodel.fields.LongField;
import org.globsframework.core.streams.accessors.LongAccessor;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.exceptions.SqlException;

import java.sql.ResultSet;
import java.sql.SQLException;

public class LongGeneratedKeyAccessor implements GeneratedKeyAccessor, LongAccessor {
    private final LongField field;
    private ResultSet resultSet;
    protected Boolean hasGeneratedKey;
    private int index;

    public LongGeneratedKeyAccessor(LongField field) {
        this.field = field;
    }

    public void setResult(ResultSet resultSet, SqlService sqlService) {
        hasGeneratedKey = true;
        this.resultSet = resultSet;
        try {
            index = resultSet.findColumn(sqlService.getColumnName(field, true));
        } catch (SQLException e) {
            throw new SqlException(e);
        }
    }

    public void reset() {
        hasGeneratedKey = false;
    }

    public Long getLong() {
        return getValue(0);
    }

    public long getValue(long valueIfNull) {
        if (hasGeneratedKey) {
            try {
                return resultSet.getLong(index);
            } catch (SQLException e) {
                throw new SqlException(e);
            }
        } else {
            throw new SqlException("No generated key for request : ");
        }
    }

    public boolean wasNull() {
        return false;
    }

    public Object getObjectValue() {
        return getLong();
    }
}
