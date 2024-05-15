package org.globsframework.sqlstreams;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.exceptions.DbConstraintViolation;
import org.globsframework.sqlstreams.exceptions.RollbackFailed;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;

public interface SqlConnection {

    SelectBuilder getQueryBuilder(GlobType globType);

    SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint);

    CreateBuilder getCreateBuilder(GlobType globType);

    UpdateBuilder getUpdateBuilder(GlobType globType, Constraint constraint);

    SqlRequest getDeleteRequest(GlobType globType);

    SqlRequest getDeleteRequest(GlobType globType, Constraint constraint);

    void commit() throws RollbackFailed, DbConstraintViolation;

    void commitAndClose() throws RollbackFailed, DbConstraintViolation;

    void rollbackAndClose();

    void createTable(GlobType globType);

    void addColumn(Field... column);

    void emptyTable(GlobType globType);

    GlobTypeExtractor extractType(String tableName);

    GlobType extractFromQuery(String query);

    default GlobType extractType(String tableName, Set<String> columnToIgnore) {
        return extractType(tableName).columnToIgnore(columnToIgnore).extract();
    }

    void populate(Collection<Glob> all);

    SqlService getJdbcSqlService();

    default void createTable(GlobType... types) {
        Arrays.stream(types).forEach(this::createTable);
    }

    default void emptyTable(GlobType... types) {
        Arrays.stream(types).forEach(this::createTable);
    }
}
