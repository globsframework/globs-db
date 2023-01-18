package org.globsframework.sqlstreams.drivers.hsqldb.impl;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;

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
