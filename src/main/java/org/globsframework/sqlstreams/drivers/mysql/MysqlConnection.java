package org.globsframework.sqlstreams.drivers.mysql;

import org.globsframework.metamodel.annotations.MaxSizeType;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.drivers.jdbc.BlobUpdater;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcConnection;
import org.globsframework.sqlstreams.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlConnection extends JdbcConnection {
    public MysqlConnection(Connection connection, SqlService sqlService) {
        super(connection, sqlService, new BlobUpdater() {
            public void setBlob(PreparedStatement preparedStatement, int index, byte[] bytes) throws SQLException {
                preparedStatement.setBytes(index, bytes);
            }
        });
    }

    protected SqlFieldCreationVisitor getFieldVisitorCreator(StringPrettyWriter prettyWriter) {
        return new SqlFieldCreationVisitor(sqlService, prettyWriter) {

            public void visitString(StringField field) throws Exception {
                Glob annotation = field.getAnnotation(MaxSizeType.KEY);
                int maxSize = 255;
                if (annotation != null) {
                    maxSize = annotation.get(MaxSizeType.VALUE, 255);
                }
                add("VARCHAR(" + maxSize + ")", field);
            }

            public String getAutoIncrementKeyWord() {
                return "AUTO_INCREMENT";
            }
        };

    }

    protected boolean isRollbackSQLState(SQLException e) {
        return e.getErrorCode() == 1099 && "HY000".equals(e.getSQLState());
    }
}
