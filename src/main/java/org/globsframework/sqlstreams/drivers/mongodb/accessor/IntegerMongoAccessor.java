package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.globsframework.streams.accessors.IntegerAccessor;
import org.globsframework.utils.Ref;

public class IntegerMongoAccessor implements IntegerAccessor {

    private final String columnName;
    private final Ref<Document> currentDoc;

    public IntegerMongoAccessor(String columnName, Ref<Document> currentDoc) {
        this.columnName = columnName;
        this.currentDoc = currentDoc;
    }

    public Integer getInteger() {
        Object value = currentDoc.get().get(columnName);
        if (value == null) {
            return null;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        throw new RuntimeException("Int type expected but got : " + value.getClass());
    }

    public int getValue(int valueIfNull) {
        Integer value = getInteger();
        return value == null ? valueIfNull : value;
    }

    public boolean wasNull() {
        return getInteger() == null;
    }

    public Object getObjectValue() {
        return getInteger();
    }
}
