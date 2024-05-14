package org.globsframework.sqlstreams.drivers.postgresql.impl;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.drivers.jdbc.impl.WhereClauseConstraintVisitor;
import org.globsframework.sqlstreams.utils.StringPrettyWriter;

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