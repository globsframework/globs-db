package org.globsframework.sql;

import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.model.*;
import org.globsframework.sql.constraints.Constraints;

public class InDbChangeSetVisitor implements ChangeSetVisitor {
    private SqlConnection sqlConnection;
    private CreateBuilder createBuilder;
    private FieldValues.Functor functorForCreate = new FieldValues.Functor() {
        public void process(Field field, Object value) throws Exception {
            createBuilder.setObject(field, value);
        }
    };

    private UpdateBuilder updateBuilder;
    private FieldValues.Functor functorForUpdate = new FieldValues.Functor() {
        public void process(Field field, Object value) throws Exception {
            updateBuilder.updateUntyped(field, value);
        }
    };

    public InDbChangeSetVisitor(SqlConnection sqlConnection) {
        this.sqlConnection = sqlConnection;
    }

    public void visitCreation(Key key, FieldsValueScanner values) throws Exception {
        createBuilder = sqlConnection.getCreateBuilder(key.getGlobType());
        key.applyOnKeyField(functorForCreate);
        values.apply(functorForCreate);
        createBuilder.getRequest().apply();
    }

    public void visitUpdate(Key key, FieldsValueWithPreviousScanner values) throws Exception {
        updateBuilder = sqlConnection.getUpdateBuilder(key.getGlobType(),
                Constraints.fieldsEqual(key.asFieldValues()));
        key.applyOnKeyField(functorForUpdate);
        values.apply(functorForUpdate);
        updateBuilder.getRequest().apply();
    }

    public void visitDeletion(Key key, FieldsValueScanner values) throws Exception {
        sqlConnection.getDeleteRequest(key.getGlobType(), Constraints.fieldsEqual(key.asFieldValues())).apply();
    }
}
