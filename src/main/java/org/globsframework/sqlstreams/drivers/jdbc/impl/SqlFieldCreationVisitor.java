package org.globsframework.sqlstreams.drivers.jdbc.impl;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.annotations.AutoIncrementAnnotationType;
import org.globsframework.metamodel.annotations.MaxSizeType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;

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
        add("INTEGER", field);
    }

    public void visitLong(LongField field) throws Exception {
        add("BIGINT", field);
    }

    public void visitDouble(DoubleField field) throws Exception {
        add("DOUBLE", field);
    }

    public void visitString(StringField field) throws Exception {
        Glob annotation = field.findAnnotation(MaxSizeType.KEY);
        int maxSize = 255;
        if (annotation != null) {
            maxSize = annotation.get(MaxSizeType.VALUE, 255);
        }
        add("VARCHAR(" + maxSize + ")", field);
    }

    public void visitBoolean(BooleanField field) throws Exception {
        add("BOOLEAN", field);
    }

    public void visitBlob(BlobField field) throws Exception {
        add("BLOB", field);
    }

    protected void add(String param, Field field) {
        boolean isAutoIncrementField = field.hasAnnotation(AutoIncrementAnnotationType.KEY);
        String columnName = sqlService.getColumnName(field);
        if (columnName != null) {
            prettyWriter
                    .append(columnName)
                    .append(" ")
                    .append(param)
                    .append(isAutoIncrementField ? " " + getAutoIncrementKeyWord() : "")
                    .appendIf(", ", appendComma);
        }
    }

    public abstract String getAutoIncrementKeyWord();
}
