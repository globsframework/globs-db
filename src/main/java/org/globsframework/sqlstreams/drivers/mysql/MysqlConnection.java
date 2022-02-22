package org.globsframework.sqlstreams.drivers.mysql;

import org.globsframework.metamodel.annotations.MaxSizeType;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.drivers.jdbc.BlobUpdater;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcConnection;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcSqlService;
import org.globsframework.sqlstreams.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class MysqlConnection extends JdbcConnection {
    public MysqlConnection(Connection connection, JdbcSqlService sqlService) {
        super(connection, sqlService, new BlobUpdater() {
            public void setBlob(PreparedStatement preparedStatement, int index, byte[] bytes) throws SQLException {
                preparedStatement.setBytes(index, bytes);
            }

            public int getBlobType() {
                return Types.BLOB;
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
        };
    }

    protected boolean isRollbackSQLState(SQLException e) {
        return e.getErrorCode() == 1099 && "HY000".equals(e.getSQLState());
    }
}
