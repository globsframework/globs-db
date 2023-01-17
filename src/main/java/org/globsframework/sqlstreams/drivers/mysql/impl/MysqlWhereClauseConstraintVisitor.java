package org.globsframework.sqlstreams.drivers.mysql.impl;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;

import java.util.Set;

public class MysqlWhereClauseConstraintVisitor  extends WhereClauseConstraintVisitor {
    public MysqlWhereClauseConstraintVisitor(StringPrettyWriter prettyWriter, SqlService sqlService, Set<GlobType> GlobeTypeSetToUpdate) {
        super(prettyWriter, sqlService, GlobeTypeSetToUpdate);
    }

    // CAST(name AS BINARY) REGEXP BINARY '^h.*$'
    public void visitRegularExpression(Field field, String value, boolean caseInsensitive, boolean not) {
        if (!caseInsensitive) {
            prettyWriter.append(" CAST(");
        }
        visitFieldOperand(field);
        if (!caseInsensitive) {
            prettyWriter.append(" AS BINARY)");
        }
        if (!caseInsensitive) {
            prettyWriter.append(not ? " NOT REGEXP BINARY" : " REGEXP BINARY");
        } else {
            prettyWriter.append(not ? " NOT REGEXP " : " REGEXP");
        }
        prettyWriter.append(" '" + value + "'");
    }
}
