package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.globsframework.sqlstreams.drivers.mongodb.MongoDbService;
import org.globsframework.streams.accessors.StringAccessor;
import org.globsframework.utils.Ref;

public class KeyStringMongoAccessor implements StringAccessor {
    private final String columnName;
    private final Ref<Document> currentDoc;

    public KeyStringMongoAccessor(String columnName, Ref<Document> currentDoc) {
        this.columnName = columnName;
        this.currentDoc = currentDoc;
    }

    public String getString() {
        return currentDoc.get().getObjectId(MongoDbService.ID_FIELD_NAME).toString();
    }

    public Object getObjectValue() {
        return getString();
    }
}
