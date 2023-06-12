package org.globsframework.sqlstreams.drivers.mysql;

import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.SelectBuilder;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.jdbc.BlobUpdater;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcConnection;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcSqlService;
import org.globsframework.sqlstreams.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sqlstreams.drivers.mysql.request.MysqlQueryBuilder;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class MysqlConnection extends JdbcConnection {
    public MysqlConnection(boolean autoCommit, Connection connection, JdbcSqlService sqlService) {
        super(autoCommit, connection, sqlService, new BlobUpdater() {
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

            public String getLongStringType() {
                return "LONGTEXT";
            }
        };
    }

    protected boolean isRollbackSQLState(SQLException e) {
        return e.getErrorCode() == 1099 && "HY000".equals(e.getSQLState());
    }
    public SelectBuilder getQueryBuilder(GlobType globType) {
        checkConnectionIsNotClosed();
        return new MysqlQueryBuilder(getConnection(), globType, null, sqlService, blobUpdater);
    }

    public SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint) {
        checkConnectionIsNotClosed();
        return new MysqlQueryBuilder(getConnection(), globType, constraint, sqlService, blobUpdater);
    }
}
