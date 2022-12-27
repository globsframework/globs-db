package org.globsframework.sqlstreams;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;

public interface SqlService {

    SqlConnection getDb();

    SqlConnection getAutoCommitDb();

    String getTableName(GlobType globType);

    String getTableName(String name);

    String getColumnName(Field field);

    String getColumnName(String field);

    String getLikeIgnoreCase();
}
