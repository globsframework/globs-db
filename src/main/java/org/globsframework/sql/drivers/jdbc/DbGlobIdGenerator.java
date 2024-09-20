package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.IntegerField;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.streams.DbStream;
import org.globsframework.core.streams.accessors.IntegerAccessor;
import org.globsframework.core.utils.Ref;
import org.globsframework.sql.CreateBuilder;
import org.globsframework.sql.SqlConnection;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.constraints.Constraint;
import org.globsframework.sql.constraints.Constraints;
import org.globsframework.sql.exceptions.RollbackFailed;

public class DbGlobIdGenerator {
    private GlobType globType;
    private StringField tableNameField;
    private IntegerField idField;
    private SqlService sqlService;

    public DbGlobIdGenerator(GlobType globType, StringField tableNameField,
                             IntegerField idField, SqlService sqlService) {
        this.globType = globType;
        this.tableNameField = tableNameField;
        this.idField = idField;
        this.sqlService = sqlService;
    }

    synchronized public int getNextId(String tableName, int idCount) {
        SqlConnection sqlConnection = sqlService.getDb();
        while (true) {
            try {
                Ref<IntegerAccessor> idRef = new Ref<IntegerAccessor>();
                Constraint constraint = Constraints.and(getAdditionalConstraint(),
                        Constraints.equal(tableNameField, tableName));
                DbStream dbStream = sqlConnection.getQueryBuilder(globType, constraint)
                        .select(idField, idRef).getQuery().execute();
                int id;
                if (dbStream.next()) {
                    id = idRef.get().getInteger() + idCount;
                    sqlConnection.getUpdateBuilder(globType, Constraints.equal(tableNameField, tableName))
                            .update(idField, id).getRequest().run();
                } else {
                    id = idCount;
                    CreateBuilder builder = sqlConnection.getCreateBuilder(globType)
                            .setObject(idField, idCount)
                            .setObject(tableNameField, tableName);
                    addAdditionalInfo(builder);
                    builder.getRequest().run();
                }
                sqlConnection.commitAndClose();
                return id - idCount;
            } catch (RollbackFailed e) {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e1) {
                }
            } finally {
                sqlConnection.rollbackAndClose();
            }
        }
    }

    protected void addAdditionalInfo(CreateBuilder builder) {
    }

    protected Constraint getAdditionalConstraint() {
        return null;
    }
}
