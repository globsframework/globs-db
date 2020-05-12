package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.globsframework.streams.accessors.StringAccessor;
import org.globsframework.streams.accessors.StringArrayAccessor;
import org.globsframework.utils.Ref;

import java.util.List;

public class StringArrayMongoAccessor implements StringArrayAccessor {
    private final String columnName;
    private final Ref<Document> currentDoc;

    public StringArrayMongoAccessor(String columnName, Ref<Document> currentDoc) {
        this.columnName = columnName;
        this.currentDoc = currentDoc;
    }

    public String[] getString() {
        Object o = currentDoc.get().get(columnName);
        if (o != null) {
            return (String[]) ((List) o).toArray();
        }
        return null;
    }

    public Object getObjectValue() {
        return getString();
    }
}
