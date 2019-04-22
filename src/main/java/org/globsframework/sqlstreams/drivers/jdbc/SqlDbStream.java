package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.metamodel.Field;
import org.globsframework.sqlstreams.accessors.SqlAccessor;
import org.globsframework.sqlstreams.exceptions.SqlException;
import org.globsframework.streams.DbStream;
import org.globsframework.streams.accessors.Accessor;
import org.globsframework.utils.exceptions.UnexpectedApplicationState;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.temporal.ChronoField;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

public class SqlDbStream implements DbStream {
    private ResultSet resultSet;
    private int rowId = 0;
    private Map<Field, SqlAccessor> fieldToAccessorHolder;
    private List<SqlAccessor> additionalAccessor;
    private SqlSelectQuery query;

    public SqlDbStream(ResultSet resultSet, Map<Field, SqlAccessor> fieldToAccessorHolder,
                       List<SqlAccessor>  additionalAccessor,
                       SqlSelectQuery query) {
        this.resultSet = resultSet;
        this.fieldToAccessorHolder = fieldToAccessorHolder;
        this.additionalAccessor = additionalAccessor;
        this.query = query;
        for (SqlAccessor sqlAccessor : fieldToAccessorHolder.values()) {
            sqlAccessor.setMoStream(this);
        }
        for (SqlAccessor sqlAccessor : additionalAccessor) {
            sqlAccessor.setMoStream(this);
        }
    }

    public boolean next() {
        try {
            rowId++;
            boolean hasNext = resultSet.next();
            if (!hasNext) {
                close();
            }
            return hasNext;
        } catch (SQLException e) {
            throw new UnexpectedApplicationState(e);
        }
    }

    public void close() {
        try {
            resultSet.close();
            query.resultSetClose();
        } catch (SQLException e) {
            throw new UnexpectedApplicationState(e);
        }
    }


    public Collection<Field> getFields() {
        return fieldToAccessorHolder.keySet();
    }

    public Accessor getAccessor(Field field) {
        return fieldToAccessorHolder.get(field);
    }

    public Double getDouble(int index) {
        try {
            Number number = ((Number) resultSet.getObject(index));
            if (number == null) {
                return null;
            }
            if (number instanceof Double) {
                return (Double) number;
            }
            return number.doubleValue();
        } catch (SQLException e) {
            throw new UnexpectedApplicationState(e);
        }
    }

    public Date getDate(int index) {
        try {
            return resultSet.getDate(index);
        } catch (SQLException e) {
            throw new UnexpectedApplicationState(e);
        }
    }

    public boolean getBoolean(int index) {
        try {
            return resultSet.getBoolean(index);
        } catch (SQLException e) {
            throw new UnexpectedApplicationState(e);
        }
    }

    public Integer getInteger(int index) {
        try {
            Object object = resultSet.getObject(index);
            if (object == null) {
                return null;
            }
            if (object instanceof Number) {
                Number number = (Number) object;
                if (number instanceof Integer) {
                    return (Integer) number;
                }
                return number.intValue();
            } else if (object instanceof Date) {
                LocalDateTime ldt = LocalDateTime.ofInstant(((Date) object).toInstant(), ZoneId.systemDefault());
                return Math.toIntExact(ldt.getLong(ChronoField.EPOCH_DAY));
            }
            throw new RuntimeException("Can not convert " + object);
        } catch (SQLException e) {
            String columnName = null;
            try {
                columnName = resultSet.getMetaData().getColumnName(index);
            } catch (SQLException e1) {
            }
            throw new SqlException("for " + columnName, e);
        }
    }

    public String getString(int index) {
        try {
            return (String) resultSet.getObject(index);
        } catch (SQLException e) {
            String columnName = null;
            try {
                columnName = resultSet.getMetaData().getColumnName(index);
            } catch (SQLException e1) {
            }
            throw new SqlException("for " + columnName, e);
        } catch (Exception e) {
            String columnName = null;
            try {
                columnName = resultSet.getMetaData().getColumnName(index);
            } catch (SQLException e1) {
            }
            throw new RuntimeException("For " + columnName, e);
        }
    }

    public Timestamp getTimeStamp(int index) {
        try {
            return resultSet.getTimestamp(index);
        } catch (SQLException e) {
            throw new SqlException(e);
        }
    }

    public byte[] getBytes(int index) {
        try {
            return resultSet.getBytes(index);
        } catch (SQLException e) {
            throw new SqlException(e);
        }
    }

    public Long getLong(int index) {
        try {
            return resultSet.getLong(index);
        } catch (SQLException e) {
            throw new SqlException(e);
        }
    }

    public boolean isNull() {
        try {
            return resultSet.wasNull();
        } catch (SQLException e) {
            throw new SqlException(e);
        }
    }

    public int getCurrentRowId() {
        return rowId;
    }
}
