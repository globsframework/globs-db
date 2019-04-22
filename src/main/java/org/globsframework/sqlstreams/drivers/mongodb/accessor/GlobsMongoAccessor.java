package org.globsframework.sqlstreams.drivers.mongodb.accessor;

import org.bson.Document;
import org.globsframework.metamodel.fields.GlobArrayField;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.drivers.mongodb.Inserter;
import org.globsframework.sqlstreams.drivers.mongodb.MongoDbService;
import org.globsframework.streams.accessors.GlobsAccessor;
import org.globsframework.utils.Ref;

import java.util.List;

public class GlobsMongoAccessor implements GlobsAccessor {
    private GlobArrayField field;
    private final Ref<Document> currentDoc;
    private MongoDbService mongoDbService;

    public GlobsMongoAccessor(GlobArrayField field, Ref<Document> currentDoc, MongoDbService mongoDbService) {
        this.field = field;
        this.currentDoc = currentDoc;
        this.mongoDbService = mongoDbService;
    }

    public Glob[] getGlobs() {
        Document document = currentDoc.get();
        Object docs = document.get(mongoDbService.getRootName(field));
        if (docs instanceof List) {
            List<Document> documents = (List<Document>) docs;
            Glob[] globs = new Glob[documents.size()];
            Inserter<Glob> inserter = new Inserter<>(globs);
            documents.forEach(document1 -> inserter.add(mongoDbService.fromDocument(field.getType(), document1)));
            return globs;
        } else {
            if (docs == null) {
                return null;
            } else {
                throw new RuntimeException("A document[] was expected but got " + docs.getClass());
            }
        }
    }

    public Object getObjectValue() {
        return getGlobs();
    }
}
