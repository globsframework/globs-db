package org.globsframework.sql;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;

public interface SqlService {

    SqlConnection getDb();

    SqlConnection getAutoCommitDb();

    String getTableName(GlobType globType, boolean escaped);

    String getTableName(String name, boolean escaped);

    String getColumnName(Field field, boolean escaped);

    String getColumnName(String field, boolean escaped);

    String getLikeIgnoreCase();
}
