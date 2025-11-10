package org.globsframework.sql.drivers.jdbc.impl;

import org.globsframework.core.metamodel.annotations.AutoIncrement;
import org.globsframework.core.metamodel.annotations.IsDate;
import org.globsframework.core.metamodel.annotations.IsDateTime;
import org.globsframework.core.metamodel.fields.*;
import org.globsframework.core.model.Glob;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.annotations.DbIsNullable;
import org.globsframework.sql.annotations.DbMaxCharSize;
import org.globsframework.sql.annotations.IsTimestamp;
import org.globsframework.sql.utils.StringPrettyWriter;

public abstract class SqlFieldCreationVisitor extends FieldVisitor.AbstractWithErrorVisitor {
    private SqlService sqlService;
    protected StringPrettyWriter prettyWriter;
    private boolean appendComma;

    public SqlFieldCreationVisitor(SqlService sqlService, StringPrettyWriter prettyWriter) {
        this.sqlService = sqlService;
        this.prettyWriter = prettyWriter;
    }

    public FieldVisitor appendComma(boolean appendComma) {
        this.appendComma = appendComma;
        return this;
    }

    public void visitInteger(IntegerField field) throws Exception {
        if (field.hasAnnotation(IsDate.KEY)) {
            add("DATE", field);
        } else {
            add("INTEGER", field);
        }
    }

    public void visitLong(LongField field) throws Exception {
        if (field.hasAnnotation(IsDate.KEY)) {
            add("DATE", field);
        } else if (field.hasAnnotation(IsDateTime.KEY)) {
            add("DATETIME", field);
        } else if (field.hasAnnotation(IsTimestamp.KEY)) {
            add("TIMESTAMP", field);
        } else {
            add("BIGINT", field);
        }
    }

    public void visitDouble(DoubleField field) throws Exception {
        add("DOUBLE", field);
    }

    public void visitString(StringField field) throws Exception {
        Glob annotation = field.findAnnotation(DbMaxCharSize.KEY);
        int maxSize = 255;
        if (annotation != null) {
            maxSize = annotation.get(DbMaxCharSize.SIZE, 255);
            if (maxSize == -1) {
                add(getLongStringType(), field);
                return;
            }
        }
        if (maxSize >= 30000) {
            add(getLongStringType(), field);
        } else {
            add("VARCHAR(" + maxSize + ")", field);
        }
    }

    public String getLongStringType() {
        return "TEXT";
    }

    public void visitStringArray(StringArrayField field) throws Exception {
        add(getLongStringType(), field);
    }

    public void visitLongArray(LongArrayField field) throws Exception {
        add(getLongStringType(), field);
    }

    public void visitBoolean(BooleanField field) throws Exception {
        add("BOOLEAN", field);
    }

    public void visitBytes(BytesField field) throws Exception {
        add("BLOB", field);
    }

    public void visitGlob(GlobField field) throws Exception {
        add(getLongStringType(), field);
    }

    public void visitGlobArray(GlobArrayField field) throws Exception {
        add(getLongStringType(), field);
    }

    public void visitUnionGlob(GlobUnionField field) throws Exception {
        add(getLongStringType(), field);
    }

    public void visitUnionGlobArray(GlobArrayUnionField field) throws Exception {
        add(getLongStringType(), field);
    }

    public void visitDate(DateField field) throws Exception {
        add("DATE", field);
    }

    public void visitDateTime(DateTimeField field) throws Exception {
        add("DATETIME", field);
    }

    protected void add(String param, Field field) {
        boolean isAutoIncrementField = field.hasAnnotation(AutoIncrement.KEY);
        String columnName = sqlService.getColumnName(field, true);
        if (columnName != null) {
            prettyWriter
                    .append(columnName)
                    .append(" ")
                    .append(param)
                    .append(isAutoIncrementField ? " " + getAutoIncrementKeyWord() : "")
                    .append(field.hasAnnotation(DbIsNullable.KEY) ? " NULL " : "")
                    .appendIf(", ", appendComma);
        }
    }

    public abstract String getAutoIncrementKeyWord();
}
