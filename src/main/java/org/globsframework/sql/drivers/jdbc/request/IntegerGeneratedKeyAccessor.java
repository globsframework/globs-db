package org.globsframework.sql.drivers.jdbc.request;

import org.globsframework.metamodel.fields.IntegerField;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.exceptions.SqlException;
import org.globsframework.streams.accessors.IntegerAccessor;

import java.sql.ResultSet;
import java.sql.SQLException;

public class IntegerGeneratedKeyAccessor implements GeneratedKeyAccessor, IntegerAccessor {
    private final IntegerField field;
    private ResultSet resultSet;
    protected Boolean hasGeneratedKey;
    private int index;

    public IntegerGeneratedKeyAccessor(IntegerField field) {
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

    public Integer getInteger() {
        return getValue(0);
    }

    public int getValue(int valueIfNull) {
        if (hasGeneratedKey) {
            try {
                return resultSet.getInt(index);
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
        return getInteger();
    }
}
