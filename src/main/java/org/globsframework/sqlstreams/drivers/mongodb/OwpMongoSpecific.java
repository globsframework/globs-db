package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.model.Updates;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.globsframework.metamodel.Field;
import org.globsframework.sqlstreams.annotations.DbRef;
import org.globsframework.streams.accessors.StringAccessor;
import org.globsframework.utils.Ref;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.globsframework.sqlstreams.drivers.mongodb.MongoUtils.DB_REF_ID_EXT;

public class OwpMongoSpecific extends MongoDbService.DefaultUpdateAdapterFactory {

    public String getMongoFieldName(Field field) {
        String dbName = super.getMongoFieldName(field);
        if (field.hasAnnotation(DbRef.KEY)) {
            return dbName + '.' + DB_REF_ID_EXT;
        } else {
            return dbName;
        }
    }

    public MongoDbService.UpdateAdapter get(Field field, MongoDbService mongoDbService) {
        if (field.hasAnnotation(DbRef.KEY)) {
            return new DefaultRefKeyUpdater(field);
        } else {
            return super.get(field, mongoDbService);
        }
    }

    static class DefaultRefKeyUpdater implements MongoDbService.UpdateAdapter {
        private final String type;
        private final String name;

        DefaultRefKeyUpdater(Field field) {
            type = field.getAnnotation(DbRef.KEY).get(DbRef.TO);
            name = MongoUtils.getDbName(field);
        }

        public void create(Object value, Document doc, MongoDbService sqlService) {
            Document document = new Document();
            document.append(DB_REF_ID_EXT, new ObjectId((String) value));
            document.append(MongoUtils.DB_REF_REF_EXT, type);
            doc.append(name, document);
        }

        public Bson update(Object value, MongoDbService sqlService) {
            Document document = new Document();
            document.append(DB_REF_ID_EXT, new ObjectId((String) value));
            document.append(MongoUtils.DB_REF_REF_EXT, type);
            return Updates.set(name, document);
        }

        public StringAccessor getAccessor(Ref<Document> currentDoc, MongoDbService sqlService) {
            return new RefStringMongoAccessor(name, currentDoc);
        }

        public Object get(Document document) {
            return null;
        }
    }

    public static class RefStringMongoAccessor implements StringAccessor {
        private static Logger LOGGER = LoggerFactory.getLogger(RefStringMongoAccessor.class);
        private final String columnName;
        private final Ref<Document> currentDoc;

        public RefStringMongoAccessor(String columnName, Ref<Document> currentDoc) {
            this.columnName = columnName;
            this.currentDoc = currentDoc;
        }

        public String getString() {
            Document document = currentDoc.get().get(columnName, Document.class);
            if (document != null) {
                ObjectId objectId = document.getObjectId(DB_REF_ID_EXT);
                if (objectId == null) {
                    String message = "Null value for " + DB_REF_ID_EXT + " in " + document + " of " + currentDoc.get();
                    LOGGER.error(message);
                    throw new RuntimeException(message);
                }
                return objectId.toString();
            }
            return null;
        }

        public Object getObjectValue() {
            return getString();
        }
    }
}
