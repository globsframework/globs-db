package org.globsframework.sql.drivers.oracle;

import org.globsframework.core.metamodel.annotations.IsDate;
import org.globsframework.core.metamodel.annotations.IsDateTime;
import org.globsframework.core.metamodel.annotations.MaxSize;
import org.globsframework.core.metamodel.fields.DoubleField;
import org.globsframework.core.metamodel.fields.LongField;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Glob;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.annotations.IsTimestamp;
import org.globsframework.sql.drivers.jdbc.BlobUpdater;
import org.globsframework.sql.drivers.jdbc.JdbcConnection;
import org.globsframework.sql.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sql.utils.StringPrettyWriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;


public class OracleConnection extends JdbcConnection {
    public OracleConnection(boolean autoCommit, Connection connection, SqlService sqlService) {
        super(autoCommit, connection, sqlService, new BlobUpdater() {
            public void setBlob(PreparedStatement preparedStatement, int index, byte[] bytes) throws SQLException {
                preparedStatement.setBytes(index, bytes);
            }

            @Override
            public int getBlobType() {
                return Types.BINARY;
            }
        });
    }

    @Override
    public void endOfRequest(StringPrettyWriter writer) {
    }

    @Override
    protected SqlFieldCreationVisitor getFieldVisitorCreator(StringPrettyWriter prettyWriter) {
        return new SqlFieldCreationVisitor(sqlService, prettyWriter) {
            @Override
            public String getAutoIncrementKeyWord() {
                return "";
            }

            @Override
            public void visitString(StringField field) throws Exception {
                Glob annotation = field.findAnnotation(MaxSize.KEY);
                int maxSize = 255;
                if (annotation != null) {
                    maxSize = annotation.get(MaxSize.VALUE, 255);
                    if (maxSize == -1) {
                        add(getLongStringType(), field);
                        return;
                    }
                }
                if (maxSize >= 30000) {
                    add(getLongStringType(), field);
                } else {
                    add("VARCHAR2(" + maxSize + ")", field);
                }
                ;
            }

            public void visitDouble(DoubleField field) throws Exception {
                add("NUMBER", field);
            }

            @Override
            public void visitLong(LongField field) throws Exception {
                if (field.hasAnnotation(IsDate.KEY)) {
                    add("DATE", field);
                } else if (field.hasAnnotation(IsDateTime.KEY)) {
                    add("TIMESTAMP WITH TIME ZONE", field);
                } else if (field.hasAnnotation(IsTimestamp.KEY)) {
                    add("TIMESTAMP", field);
                } else {
                    add("BIGINT", field);
                }
                ;
            }
        };
    }
}
