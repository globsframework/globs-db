package org.globsframework.sqlstreams.drivers.jdbc.impl;

import org.globsframework.json.GSonUtils;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.Glob;
import org.globsframework.metamodel.annotations.IsDate;
import org.globsframework.metamodel.annotations.IsDateTime;
import org.globsframework.sqlstreams.annotations.IsTimestamp;
import org.globsframework.sqlstreams.drivers.jdbc.BlobUpdater;
import org.globsframework.utils.Strings;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.Arrays;

public class SqlValueFieldVisitor extends FieldVisitor.AbstractWithErrorVisitor {
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
        if (field.hasAnnotation(IsDate.KEY)) {
            if (value == null) {
                preparedStatement.setNull(index, Types.DATE);
            } else {
                preparedStatement.setDate(index, new Date(((Integer) value) * 24L * 3600L * 1000L));
            }
        } else {
            if (value == null) {
                preparedStatement.setNull(index, Types.INTEGER);
            } else {
                preparedStatement.setInt(index, (Integer) value);
            }
        }
    }

    public void visitDate(DateField field) throws Exception {
        if (value == null) {
            preparedStatement.setNull(index, Types.DATE);
        } else {
            preparedStatement.setDate(index, Date.valueOf(((LocalDate) value)));
        }
    }

    public void visitDateTime(DateTimeField field) throws Exception {
        if (value == null) {
            preparedStatement.setNull(index, Types.TIMESTAMP);
        } else {
            preparedStatement.setTimestamp(index, Timestamp.from(((ZonedDateTime) value).toInstant()));
        }
    }

    public void visitLong(LongField field) throws Exception {
        if (field.hasAnnotation(IsDate.KEY)) {
            if (value == null) {
                preparedStatement.setNull(index, Types.DATE);
            } else {
                preparedStatement.setDate(index, new Date(((Integer) value) * 24L * 3600L * 1000L));
            }
        } else if (field.hasAnnotation(IsDateTime.KEY)) {
            if (value == null) {
                preparedStatement.setNull(index, Types.TIMESTAMP);
            } else {
                preparedStatement.setTimestamp(index, new Timestamp((Long) value));
            }
        } else if (field.hasAnnotation(IsTimestamp.KEY)) {
            if (value == null) {
                preparedStatement.setNull(index, Types.TIMESTAMP);
            } else {
                preparedStatement.setTimestamp(index, new Timestamp((Long) value));
            }
        } else {
            if (value == null) {
                preparedStatement.setNull(index, Types.BIGINT);
            } else {
                preparedStatement.setLong(index, (Long) value);
            }
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

    public void visitStringArray(StringArrayField field) throws Exception {
        if (value == null) {
            preparedStatement.setNull(index, Types.VARCHAR);
        } else {
            preparedStatement.setString(index, Strings.joinWithSeparator(",", Arrays.asList((String[]) value)));
        }
    }

    public void visitBoolean(BooleanField field) throws Exception {
        if (value == null) {
            preparedStatement.setNull(index, Types.BOOLEAN);
        } else {
            preparedStatement.setBoolean(index, (Boolean) value);
        }
    }

    public void visitGlob(GlobField field) throws Exception {
        if (value == null) {
            preparedStatement.setNull(index, Types.VARCHAR);
        } else {
            preparedStatement.setString(index, GSonUtils.encode((Glob) this.value, true));
        }
    }

    public void visitGlobArray(GlobArrayField field) throws Exception {
        if (value == null) {
            preparedStatement.setNull(index, Types.VARCHAR);
        } else {
            preparedStatement.setString(index, GSonUtils.encode((Glob[]) value, true));
        }
    }

    public void visitUnionGlob(GlobUnionField field) throws Exception {
        if (value == null) {
            preparedStatement.setNull(index, Types.VARCHAR);
        } else {
            preparedStatement.setString(index, GSonUtils.encode((Glob) this.value, true));
        }
    }

    public void visitUnionGlobArray(GlobArrayUnionField field) throws Exception {
        if (value == null) {
            preparedStatement.setNull(index, Types.VARCHAR);
        } else {
            preparedStatement.setString(index, GSonUtils.encode((Glob[]) value, true));
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
