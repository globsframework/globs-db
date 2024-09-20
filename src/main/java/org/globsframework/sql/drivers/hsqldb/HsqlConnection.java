package org.globsframework.sql.drivers.hsqldb;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.BlobField;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.SelectBuilder;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.hsqldb.request.HsqldbSqlQueryBuilder;
import org.globsframework.sql.drivers.jdbc.BlobUpdater;
import org.globsframework.sql.drivers.jdbc.JdbcConnection;
import org.globsframework.sql.drivers.jdbc.JdbcSqlService;
import org.globsframework.sql.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sql.utils.StringPrettyWriter;
import org.hsqldb.jdbc.JDBCBlob;
import org.hsqldb.util.DatabaseManagerSwing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class HsqlConnection extends JdbcConnection {
    public HsqlConnection(boolean autoCommit, Connection connection, JdbcSqlService sqlService) {
        super(autoCommit, connection, sqlService, new BlobUpdater() {
            public void setBlob(PreparedStatement preparedStatement, int index, byte[] bytes) throws SQLException {
                preparedStatement.setBlob(index, new JDBCBlob(bytes));
            }

            public int getBlobType() {
                return Types.LONGNVARCHAR;
            }
        });
    }

    protected SqlFieldCreationVisitor getFieldVisitorCreator(StringPrettyWriter prettyWriter) {
        return new SqlFieldCreationVisitor(sqlService, prettyWriter) {
            public String getAutoIncrementKeyWord() {
                return "IDENTITY";
            }

            public void visitBlob(BlobField field) {
                add("LONGVARBINARY", field);
            }

            public String getLongStringType() {
                return "LONGVARCHAR";
            }
        };
    }

    public SelectBuilder getQueryBuilder(GlobType globType) {
        checkConnectionIsNotClosed();
        return new HsqldbSqlQueryBuilder(getConnection(), globType, null, sqlService, blobUpdater);
    }

    public SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint) {
        checkConnectionIsNotClosed();
        return new HsqldbSqlQueryBuilder(getConnection(), globType, constraint, sqlService, blobUpdater);
    }

    // hsql db do not support add of multiple column
    public void addColumn(Field... column) {
        for (Field field : column) {
            super.addColumn(field);
        }
    }

    public void showDb() {
        Thread thread = new Thread() {

            public void run() {
                try {
                    DatabaseManagerSwing.main(new String[0]);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };
        thread.setDaemon(true);
        thread.start();
    }
}
