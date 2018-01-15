package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.globsframework.sqlstreams.drivers.mongodb.MongoUtils;
import org.globsframework.streams.accessors.StringAccessor;
import org.globsframework.utils.Ref;

public class RefStringMongoAccessor implements StringAccessor {
    private final String columnName;
    private final Ref<Document> currentDoc;

    public RefStringMongoAccessor(String columnName, Ref<Document> currentDoc) {
        this.columnName = columnName;
        this.currentDoc = currentDoc;
    }

    public String getString() {
        Document document = currentDoc.get().get(columnName, Document.class);
        if (document != null) {
            return document.getObjectId(MongoUtils.DB_REF_ID_EXT).toString();
        }
        return null;
    }

    public Object getObjectValue() {
        return getString();
    }
}
