package org.globsframework.sql.drivers.postgresql;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.annotations.AutoIncrement;
import org.globsframework.core.metamodel.annotations.IsDate;
import org.globsframework.core.metamodel.annotations.IsDateTime;
import org.globsframework.core.metamodel.annotations.MaxSize;
import org.globsframework.core.metamodel.fields.*;
import org.globsframework.core.model.Glob;
import org.globsframework.sql.SelectBuilder;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.annotations.IsTimestamp;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.BlobUpdater;
import org.globsframework.sql.drivers.jdbc.JdbcConnection;
import org.globsframework.sql.drivers.jdbc.JdbcSqlService;
import org.globsframework.sql.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sql.drivers.postgresql.request.PostgreSqlQueryBuilder;
import org.globsframework.sql.utils.StringPrettyWriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class PostgresqlConnection extends JdbcConnection {
    public PostgresqlConnection(boolean autoCommit, Connection connection, SqlService sqlService) {
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

    protected SqlFieldCreationVisitor getFieldVisitorCreator(StringPrettyWriter prettyWriter) {
        return new SqlFieldCreationVisitor(sqlService, prettyWriter) {

            public String getAutoIncrementKeyWord() {
                return "";
            }

            @Override
            public String getLongStringType() {
                return "TEXT";
            }

            @Override
            public void visitLong(LongField field) throws Exception {
                if (field.hasAnnotation(AutoIncrement.KEY)) {
                    add("BIGSERIAL", field);
                } else if (field.hasAnnotation(IsDate.KEY)) {
                    add("DATE", field);
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

            public void visitInteger(IntegerField field) throws Exception {
                if (field.hasAnnotation(AutoIncrement.KEY)) {
                    add("SERIAL", field);
                } else {
                    super.visitInteger(field);
                }
            }

            public void visitString(StringField field) throws Exception {
                Glob annotation = field.findAnnotation(MaxSize.KEY);
                int maxSize = 255;
                if (annotation != null) {
                    maxSize = annotation.get(MaxSize.VALUE, 255);
                    if (maxSize == -1) {
                        add("TEXT", field);
                        return;
                    }
                }
                if (maxSize >= 30000) {
                    add(getLongStringType(), field);
                } else {
                    add("CHARACTER VARYING(" + maxSize + ")", field);
                }
            }

            @Override
            public void visitBlob(BlobField field) throws Exception {
                add("BYTEA", field);
            }

            @Override
            public void visitDateTime(DateTimeField field) throws Exception {
                add("TIMESTAMP WITH TIME ZONE", field);
            }
        };
    }

    public SelectBuilder getQueryBuilder(GlobType globType) {
        checkConnectionIsNotClosed();
        return new PostgreSqlQueryBuilder(getConnection(), globType, null, sqlService, blobUpdater);
    }

    public SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint) {
        checkConnectionIsNotClosed();
        return new PostgreSqlQueryBuilder(getConnection(), globType, constraint, sqlService, blobUpdater);
    }

}
