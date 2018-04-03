package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.async.client.MongoCollection;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.drivers.jdbc.AccessorGlobBuilder;
import org.globsframework.streams.GlobStream;
import org.globsframework.streams.accessors.Accessor;
import org.globsframework.utils.Ref;
import org.globsframework.utils.exceptions.ItemNotFound;
import org.globsframework.utils.exceptions.TooManyItems;

import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Spliterators;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Projections.include;

public class MongoSelectQuery implements SelectQuery {
    private final MongoCollection<Document> collection;
    private final Map<Field, Accessor> fieldsAndAccessor;
    private final Ref<Document> currentDoc;
    private final GlobType globType;
    private final String requete;
    private SqlService sqlService;


    public MongoSelectQuery(MongoCollection<Document> collection, Map<Field, Accessor> fieldsAndAccessor,
                            Ref<Document> currentDoc, GlobType globType, SqlService sqlService) {
        this.collection = collection;
        this.fieldsAndAccessor = fieldsAndAccessor;
        this.currentDoc = currentDoc;
        this.globType = globType;
        requete = "select on " + globType.getName();
        this.sqlService = sqlService;
    }


    static final Document END = new Document();

    public Stream<Object> executeAsStream() {
        DocumentsIterator iterator = getDocumentsIterator();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), true);
    }

    private DocumentsIterator getDocumentsIterator() {
        LinkedBlockingDeque<Document> objects = new LinkedBlockingDeque<>();
        Bson include = include(fieldsAndAccessor.keySet()
                .stream().map(sqlService::getColumnName).collect(Collectors.toList()));
        collection.find()
                .projection(include)
                .forEach(document -> {
                    if (document != null) {
                        objects.add(document);
                    }
                }, (aVoid, throwable) -> {
                    if (throwable != null) {
                        throwable.printStackTrace();
                    }
                    objects.add(END);
                });
        return new DocumentsIterator(currentDoc, objects);
    }

    public GlobStream execute() {
        DocumentsIterator iterator = getDocumentsIterator();
        return new GlobStream() {
            public boolean next() {
                if (iterator.hasNext()) {
                    iterator.next();
                    return true;
                }
                return false;
            }

            public Collection<Field> getFields() {
                return fieldsAndAccessor.keySet();
            }

            public Accessor getAccessor(Field field) {
                return fieldsAndAccessor.get(field);
            }

            public void close() {

            }
        };
    }

    public GlobList executeAsGlobs() {
        GlobStream globStream = execute();
        AccessorGlobBuilder accessorGlobBuilder = AccessorGlobBuilder.init(globStream);
        GlobList result = new GlobList();
        while (globStream.next()) {
            result.addAll(accessorGlobBuilder.getGlobs());
        }
        return result;
    }

    public Glob executeUnique() throws ItemNotFound, TooManyItems {
        GlobList globs = executeAsGlobs();
        if (globs.size() == 1) {
            return globs.get(0);
        }
        if (globs.isEmpty()) {
            throw new ItemNotFound("No result returned for: " + requete);
        }
        throw new TooManyItems("Too many results for: " + requete);
    }

    public void close() {
    }

    private static class DocumentsIterator implements Iterator<Object> {
        private Ref<Document> currentDoc;
        private final LinkedBlockingDeque<Document> documents;
        Document current;

        public DocumentsIterator(Ref<Document> currentDoc, LinkedBlockingDeque<Document> documents) {
            this.currentDoc = currentDoc;
            this.documents = documents;
            current = null;
        }

        public boolean hasNext() {
            if (current == END) {
                currentDoc.set(null);
                return false;
            }
            if (current != null) {
                return true;
            }
            current = documents.poll();
            return current != END;
        }

        public Object next() {
            Document current = this.current;
            this.current = null;
            currentDoc.set(current);
            return current;
        }
    }
}
