package org.globsframework.sql.drivers.hsqldb;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.BytesField;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.SelectBuilder;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.drivers.hsqldb.request.HsqldbSqlQueryBuilder;
import org.globsframework.sql.drivers.jdbc.JdbcConnection;
import org.globsframework.sql.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sql.utils.StringPrettyWriter;

import java.sql.Connection;

import org.globsframework.sql.Upsert;
import java.util.List;
import java.util.function.Function;
public class HsqlConnection extends JdbcConnection {
    public HsqlConnection(boolean autoCommit, Connection connection, SqlService sqlService) {
        super(autoCommit, connection, sqlService);
    }

    protected SqlFieldCreationVisitor getFieldVisitorCreator(StringPrettyWriter prettyWriter) {
        return new SqlFieldCreationVisitor(sqlService, prettyWriter) {
            public String getAutoIncrementKeyWord() {
                return "IDENTITY";
            }

            public void visitBytes(BytesField field) {
                add("LONGVARBINARY", field);
            }

            public String getLongStringType() {
                return "LONGVARCHAR";
            }
        };
    }

    public SelectBuilder getQueryBuilder(GlobType globType) {
        checkConnectionIsNotClosed();
        return new HsqldbSqlQueryBuilder(getConnection(), globType, null, sqlService);
    }

    public SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint) {
        checkConnectionIsNotClosed();
        return new HsqldbSqlQueryBuilder(getConnection(), globType, constraint, sqlService);
    }

    // hsql db do not support add of multiple column
    public void addColumn(Field... column) {
        for (Field field : column) {
            super.addColumn(field);
        }
    }

    public String upsertRequest(GlobType globType, List<Field> columns, Upsert upsert,
                                Function<Field, String> placeholder) {
        return mergeRequest(globType, columns, upsert, placeholder, "src");
    }
}
