package org.globsframework.sql.drivers.jdbc.request;

import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.streams.accessors.StringAccessor;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.exceptions.SqlException;

import java.sql.ResultSet;
import java.sql.SQLException;

public class StringGeneratedKeyAccessor implements GeneratedKeyAccessor, StringAccessor {
    private final StringField field;
    private ResultSet resultSet;
    protected Boolean hasGeneratedKey;
    private int index;

    public StringGeneratedKeyAccessor(StringField field) {
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


    public String getString() {
        if (hasGeneratedKey) {
            try {
                return resultSet.getString(index);
            } catch (SQLException e) {
                throw new SqlException(e);
            }
        } else {
            throw new SqlException("No generated key.");
        }
    }

    public boolean wasNull() {
        return false;
    }

    public Object getObjectValue() {
        return getString();
    }
}
