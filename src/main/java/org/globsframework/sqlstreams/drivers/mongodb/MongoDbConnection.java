package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.MongoDatabase;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.*;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.exceptions.DbConstraintViolation;
import org.globsframework.sqlstreams.exceptions.RollbackFailed;

public class MongoDbConnection implements SqlConnection {
    MongoDatabase mongoDatabase;
    MongoDbService sqlService;

    public MongoDbConnection(MongoDatabase mongoDatabase, MongoDbService sqlService) {
        this.mongoDatabase = mongoDatabase;
        this.sqlService = sqlService;
    }

    public SelectBuilder getQueryBuilder(GlobType globType) {
        return new MongoSelectBuilder(mongoDatabase, globType, sqlService, null);
    }

    public SelectBuilder getQueryBuilder(GlobType globType, Constraint constraint) {
        return new MongoSelectBuilder(mongoDatabase, globType, sqlService, constraint);
    }

    public CreateBuilder getCreateBuilder(GlobType globType) {
        return new MongoCreateBuilder(mongoDatabase, globType, sqlService);
    }

    public UpdateBuilder getUpdateBuilder(GlobType globType, Constraint constraint) {
        return new MongoUpdateBuilder(mongoDatabase, globType, sqlService, constraint);
    }

    public SqlRequest getDeleteRequest(GlobType globType) {
        throw new RuntimeException("Not Implemented");
    }

    public SqlRequest getDeleteRequest(GlobType globType, Constraint constraint) {
        throw new RuntimeException("Not Implemented");
    }

    public void commit() throws RollbackFailed, DbConstraintViolation {
    }

    public void commitAndClose() throws RollbackFailed, DbConstraintViolation {

    }

    public void rollbackAndClose() {

    }

    public GlobTypeExtractor extractType(String tableName) {
        throw new RuntimeException("Not Implemented");
    }

    public void createTable(GlobType globType) {
        throw new RuntimeException("Not Implemented");
    }

    public void addColumn(Field... column) {
        throw new RuntimeException("Not Implemented");
    }

    public void emptyTable(GlobType globType) {
        throw new RuntimeException("Not Implemented");
    }

    public void showDb() {
    }

    public void populate(GlobList all) {
        MongoUtils.fill(all, sqlService);
    }

    public SqlService getJdbcSqlService() {
        return sqlService;
    }

    public interface IsComplete {
        boolean complete();
    }

    public static boolean waitComplete(Object thisObject, IsComplete isComplete, int timeInSecond) {
        synchronized (thisObject) {
            long waitUntil = System.currentTimeMillis() + timeInSecond * 1000;
            while (!isComplete.complete()) {
                long stillToWait = waitUntil - System.currentTimeMillis();
                if (stillToWait <= 0) {
                    return false;
                }
                try {
                    thisObject.wait(stillToWait);
                } catch (InterruptedException e) {
                    return false;
                }
            }
        }
        return true;
    }

}
