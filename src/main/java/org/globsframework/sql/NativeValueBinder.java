package org.globsframework.sql;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

/**
 * How a string reaches a column that is not a string column — a {@code uuid}, a {@code jsonb}, an
 * {@code inet}.
 * <p>
 * It exists because PostgreSQL refuses {@code setString} into any non-text column ("column is of
 * type uuid but expression is of type character varying"). Sending the parameter untyped instead
 * leaves the server to read it as whatever the column is, which works for every type including the
 * text ones. Every other dialect this library speaks accepts {@code setString} and keeps it.
 */
public interface NativeValueBinder {

    NativeValueBinder AS_STRING = PreparedStatement::setString;

    /**
     * Sends the value with no type of its own, for the server to interpret from the column.
     */
    NativeValueBinder UNTYPED = (statement, index, value) -> statement.setObject(index, value, Types.OTHER);

    void bind(PreparedStatement statement, int index, String value) throws SQLException;
}
