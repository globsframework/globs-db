package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.globsframework.streams.accessors.StringAccessor;
import org.globsframework.utils.Ref;

public class StringMongoAccessor implements StringAccessor {
    private final String columnName;
    private final Ref<Document> currentDoc;

    public StringMongoAccessor(String columnName, Ref<Document> currentDoc) {
        this.columnName = columnName;
        this.currentDoc = currentDoc;
    }

    public String getString() {
        return currentDoc.get().getString(columnName);
    }

    public Object getObjectValue() {
        return getString();
    }
}
