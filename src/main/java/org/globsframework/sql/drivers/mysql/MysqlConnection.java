package org.globsframework.sql.drivers.mysql;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.sql.SelectBuilder;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.jdbc.JdbcConnection;
import org.globsframework.sql.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sql.drivers.mysql.request.MysqlQueryBuilder;
import org.globsframework.sql.utils.StringPrettyWriter;

import java.sql.Connection;
import java.sql.SQLException;

public class MysqlConnection extends JdbcConnection {
    public MysqlConnection(boolean autoCommit, Connection connection, SqlService sqlService) {
        super(autoCommit, connection, sqlService);
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
        return new MysqlQueryBuilder(getConnection(), globType, null, sqlService);
    }

    public SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint) {
        checkConnectionIsNotClosed();
        return new MysqlQueryBuilder(getConnection(), globType, constraint, sqlService);
    }
}
