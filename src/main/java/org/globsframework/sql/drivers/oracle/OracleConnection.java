package org.globsframework.sql.drivers.oracle;

import org.globsframework.core.metamodel.annotations.IsDate;
import org.globsframework.core.metamodel.annotations.IsDateTime;
import org.globsframework.core.metamodel.fields.DoubleField;
import org.globsframework.core.metamodel.fields.LongField;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.model.Glob;
import org.globsframework.sql.SelectBuilder;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.annotations.DbMaxCharSize;
import org.globsframework.sql.annotations.IsTimestamp;
import org.globsframework.sql.drivers.jdbc.JdbcConnection;
import org.globsframework.sql.drivers.jdbc.impl.SqlFieldCreationVisitor;
import org.globsframework.sql.drivers.oracle.request.OracleSqlQueryBuilder;
import org.globsframework.sql.utils.StringPrettyWriter;

import java.sql.Connection;


import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.Upsert;
import java.util.List;
import java.util.function.Function;
public class OracleConnection extends JdbcConnection {
    public OracleConnection(boolean autoCommit, Connection connection, SqlService sqlService) {
        super(autoCommit, connection, sqlService);
    }

    @Override
    public SelectBuilder getQueryBuilder(GlobType globType) {
        checkConnectionIsNotClosed();
        return new OracleSqlQueryBuilder(getConnection(), globType, null, sqlService);
    }

    @Override
    public SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint) {
        checkConnectionIsNotClosed();
        return new OracleSqlQueryBuilder(getConnection(), globType, constraint, sqlService);
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
                Glob annotation = field.findAnnotation(DbMaxCharSize.KEY);
                int maxSize = 255;
                if (annotation != null) {
                    maxSize = annotation.get(DbMaxCharSize.SIZE, 255);
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

    public String upsertRequest(GlobType globType, List<Field> columns, Upsert upsert,
                                Function<Field, String> placeholder) {
        return mergeRequest(globType, columns, upsert, placeholder, "src");
    }

    /**
     * Oracle has no standalone VALUES row constructor: the source is a one row query on dual.
     */
    protected String mergeSource(List<Field> columns, Function<Field, String> placeholder, String alias) {
        StringPrettyWriter writer = new StringPrettyWriter();
        writer.append("(SELECT ");
        appendColumns(writer, columns, ", ", field -> placeholder.apply(field) + " " + column(field));
        writer.append(" FROM dual) ").append(alias);
        return writer.toString();
    }
}
