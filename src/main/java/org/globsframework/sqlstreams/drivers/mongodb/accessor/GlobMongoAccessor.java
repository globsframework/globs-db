package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.globsframework.metamodel.fields.GlobField;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.drivers.mongodb.MongoDbService;
import org.globsframework.streams.accessors.GlobAccessor;
import org.globsframework.utils.Ref;

public class GlobMongoAccessor implements GlobAccessor {
    private GlobField field;
    private final Ref<Document> currentDoc;
    private MongoDbService mongoDbService;

    public GlobMongoAccessor(GlobField field, Ref<Document> currentDoc, MongoDbService mongoDbService) {
        this.field = field;
        this.currentDoc = currentDoc;
        this.mongoDbService = mongoDbService;
    }

    public Glob getGlob() {
        Document document = currentDoc.get();
        Object o = document.get(mongoDbService.getRootName(field));
        if (o instanceof Document) {
            return mongoDbService.fromDocument(field.getType(), (Document) o);
        } else {
            if (o == null) {
                return null;
            } else {
                throw new RuntimeException("A document[] was expected but got " + o.getClass());
            }
        }
    }

    public Object getObjectValue() {
        return getGlob();
    }
}
