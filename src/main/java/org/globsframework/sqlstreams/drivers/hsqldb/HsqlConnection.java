package org.globsframework.sqlstreams.drivers.hsqldb;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.fields.*;
import org.globsframework.sqlstreams.drivers.jdbc.BlobUpdater;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcConnection;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcSqlService;
import org.globsframework.sqlstreams.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;
import org.hsqldb.jdbc.jdbcBlob;
import org.hsqldb.util.DatabaseManagerSwing;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class HsqlConnection extends JdbcConnection {
    public HsqlConnection(boolean autoCommit, Connection connection, JdbcSqlService sqlService) {
        super(autoCommit, connection, sqlService, new BlobUpdater() {
            public void setBlob(PreparedStatement preparedStatement, int index, byte[] bytes) throws SQLException {
                preparedStatement.setBlob(index, new jdbcBlob(bytes));
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

            public void visitGlob(GlobField field) {
                add("LONGVARCHAR", field);
            }

            public void visitGlobArray(GlobArrayField field) {
                add("LONGVARCHAR", field);
            }

            public void visitStringArray(StringArrayField field) throws Exception {
                add("LONGVARCHAR", field);
            }

            public void visitLongArray(LongArrayField field) throws Exception {
                add("LONGVARCHAR", field);
            }

            public void visitUnionGlobArray(GlobArrayUnionField field) throws Exception {
                add("LONGVARCHAR", field);
            }

            public void visitUnionGlob(GlobUnionField field) throws Exception {
                add("LONGVARCHAR", field);
            }
        };
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
