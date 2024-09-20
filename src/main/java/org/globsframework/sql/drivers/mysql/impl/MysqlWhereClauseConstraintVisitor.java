package org.globsframework.sql.drivers.mysql.impl;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sql.utils.StringPrettyWriter;

import java.util.Set;

public class MysqlWhereClauseConstraintVisitor extends WhereClauseConstraintVisitor {
    public MysqlWhereClauseConstraintVisitor(StringPrettyWriter prettyWriter, SqlService sqlService, Set<GlobType> GlobeTypeSetToUpdate) {
        super(prettyWriter, sqlService, GlobeTypeSetToUpdate);
    }

    // CAST(name AS BINARY) REGEXP BINARY '^h.*$'
    public void visitRegularExpression(Field field, String value, boolean caseSensitive, boolean not) {
        if (caseSensitive) {
            prettyWriter.append(" CAST(");
        }
        visitFieldOperand(field);
        if (caseSensitive) {
            prettyWriter.append(" AS BINARY)");
        }
        if (caseSensitive) {
            prettyWriter.append(not ? " NOT REGEXP CAST(? AS BINARY)" : " REGEXP CAST(? AS BINARY)");
        } else {
            prettyWriter.append(not ? " NOT REGEXP ?" : " REGEXP ?");
        }
    }
}
