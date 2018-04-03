package org.globsframework.sqlstreams.drivers.jdbc.impl;

import org.globsframework.metamodel.fields.*;
import org.globsframework.sqlstreams.drivers.jdbc.BlobUpdater;

import java.sql.PreparedStatement;
import java.sql.Types;

public class SqlValueFieldVisitor implements FieldVisitor {
    private PreparedStatement preparedStatement;
    private BlobUpdater blobUpdater;
    private Object value;
    private int index;

    public SqlValueFieldVisitor(PreparedStatement preparedStatement, BlobUpdater blobUpdater) {
        this.preparedStatement = preparedStatement;
        this.blobUpdater = blobUpdater;
    }

    public void setValue(Object value, int index) {
        this.value = value;
        this.index = index;
    }

    public void visitInteger(IntegerField field) throws Exception {
        if (value == null) {
            preparedStatement.setNull(index, Types.INTEGER);
        } else {
            preparedStatement.setInt(index, (Integer) value);
        }
    }

    public void visitLong(LongField field) throws Exception {
        if (value == null) {
            preparedStatement.setNull(index, Types.BIGINT);
        } else {
            preparedStatement.setLong(index, (Long) value);
        }
    }

    public void visitDouble(DoubleField field) throws Exception {
        if (value == null) {
            preparedStatement.setNull(index, Types.DOUBLE);
        } else {
            preparedStatement.setDouble(index, (Double) value);
        }
    }

    public void visitString(StringField field) throws Exception {
        if (value == null) {
            preparedStatement.setNull(index, Types.VARCHAR);
        } else {
            preparedStatement.setString(index, (String) value);
        }
    }

    public void visitBoolean(BooleanField field) throws Exception {
        if (value == null) {
            preparedStatement.setNull(index, Types.BOOLEAN);
        } else {
            preparedStatement.setBoolean(index, (Boolean) value);
        }
    }

    public void visitBlob(BlobField field) throws Exception {
        if (value == null) {
            preparedStatement.setNull(index, Types.BLOB);
        } else {
            blobUpdater.setBlob(preparedStatement, index, ((byte[]) value));
        }
    }
}
