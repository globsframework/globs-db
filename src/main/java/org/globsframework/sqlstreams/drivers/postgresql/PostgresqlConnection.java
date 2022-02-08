package org.globsframework.sqlstreams.drivers.postgresql;

import org.globsframework.metamodel.annotations.IsDate;
import org.globsframework.metamodel.annotations.IsDateTime;
import org.globsframework.metamodel.annotations.MaxSizeType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.annotations.IsTimestamp;
import org.globsframework.sqlstreams.drivers.jdbc.BlobUpdater;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcConnection;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcSqlService;
import org.globsframework.sqlstreams.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PostgresqlConnection extends JdbcConnection {
    public PostgresqlConnection(Connection connection, JdbcSqlService sqlService) {
        super(connection, sqlService, new BlobUpdater() {
            public void setBlob(PreparedStatement preparedStatement, int index, byte[] bytes) throws SQLException {
                preparedStatement.setBytes(index, bytes);
            }
        });
    }

    protected SqlFieldCreationVisitor getFieldVisitorCreator(StringPrettyWriter prettyWriter) {
        return new SqlFieldCreationVisitor(sqlService, prettyWriter) {

            public String getAutoIncrementKeyWord() {
                return "AUTO_INCREMENT";
            }

            public String getLongStringType(int maxSize) {
                return "TEXT";
            }

            public void visitLong(LongField field) throws Exception {
                if (field.hasAnnotation(IsDate.KEY)) {
                    add("DATE",field);
                } else if (field.hasAnnotation(IsDateTime.KEY)) {
                    add("TIMESTAMP WITH TIME ZONE", field);
                } else if (field.hasAnnotation(IsTimestamp.KEY)) {
                    add("TIMESTAMP", field);
                } else {
                    add("BIGINT", field);
                }
            }

            public void visitDouble(DoubleField field) {
                add("DOUBLE PRECISION", field);
            }

            public void visitString(StringField field) throws Exception {
                Glob annotation = field.findAnnotation(MaxSizeType.KEY);
                int maxSize = 255;
                if (annotation != null) {
                    maxSize = annotation.get(MaxSizeType.VALUE, 255);
                }
                if (maxSize >= 30000) {
                    add(getLongStringType(maxSize), field);
                }
                else {
                    add("CHARACTER VARYING(" + maxSize + ")", field);
                }
            }

            public void visitBlob(BlobField field) throws Exception {
                add("BYTEA", field);
            }

            public void visitUnionGlob(GlobUnionField field) throws Exception {
                add("TEXT", field);
            }

            public void visitUnionGlobArray(GlobArrayUnionField field) throws Exception {
                add("TEXT", field);
            }

            public void visitDateTime(DateTimeField field) throws Exception {
                add("TIMESTAMP WITH TIME ZONE", field);
            }

        };
    }
}
