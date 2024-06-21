package org.globsframework.sql.drivers.hsqldb.impl;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sql.utils.StringPrettyWriter;

import java.util.Set;

public class HsqldbWhereClauseConstraintVisitor extends WhereClauseConstraintVisitor {
    public HsqldbWhereClauseConstraintVisitor(StringPrettyWriter prettyWriter, SqlService sqlService, Set<GlobType> GlobeTypeSetToUpdate) {
        super(prettyWriter, sqlService, GlobeTypeSetToUpdate);
    }

    public void visitRegularExpression(Field field, String value, boolean caseSensitive, boolean not) {
        if(not) {
            prettyWriter.append(" NOT ");
        }
        prettyWriter.append("REGEXP_MATCHES(");
        visitFieldOperand(field);
        prettyWriter.append(",?)");
    }
}
