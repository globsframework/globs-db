package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.fields.GlobField;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.drivers.mongodb.MongoDbService;
import org.globsframework.streams.accessors.BlobAccessor;
import org.globsframework.streams.accessors.GlobAccessor;
import org.globsframework.utils.Ref;

import java.util.Base64;

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
        return mongoDbService.fromDocument(field.getType(), currentDoc.get());
    }

    public Object getObjectValue() {
        return getGlob();
    }
}
