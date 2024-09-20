package org.globsframework.sql.drivers.postgresql.impl;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sql.utils.StringPrettyWriter;

import java.util.Set;

public class PostgreWhereClauseConstraintVisitor extends WhereClauseConstraintVisitor {
    public PostgreWhereClauseConstraintVisitor(StringPrettyWriter prettyWriter, SqlService sqlService, Set<GlobType> GlobeTypeSetToUpdate) {
        super(prettyWriter, sqlService, GlobeTypeSetToUpdate);
    }

    public void visitRegularExpression(Field field, String value, boolean caseSensitive, boolean not) {
        visitFieldOperand(field);
        if (caseSensitive) {
            prettyWriter.append(not ? " !~" : " ~");
        } else {
            prettyWriter.append(not ? " !~*" : " ~*");
        }
        prettyWriter.append(" ? ");
    }
}
