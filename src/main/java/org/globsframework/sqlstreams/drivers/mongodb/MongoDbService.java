package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.async.client.MongoDatabase;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.utils.AbstractSqlService;

public class MongoDbService extends AbstractSqlService {
    private final MongoDatabase database;

    public MongoDbService(MongoDatabase database) {
        this.database = database;
    }

    public SqlConnection getDb() {
        return new MongoDbConnection(database, this);
    }


    public String getTableName(GlobType globType) {
        return globType.getName();
    }

    public String getColumnName(Field field) {
        return MongoUtils.getFullDbName(field);
    }

    public String getFirstLevelColumnName(Field field) {
        return MongoUtils.getDbName(field);
    }

}
