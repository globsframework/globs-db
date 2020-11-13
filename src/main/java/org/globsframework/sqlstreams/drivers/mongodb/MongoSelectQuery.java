package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Sorts;
import org.bson.BsonDocument;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.drivers.jdbc.AccessorGlobBuilder;
import org.globsframework.streams.DbStream;
import org.globsframework.streams.accessors.Accessor;
import org.globsframework.utils.Ref;
import org.globsframework.utils.exceptions.ItemNotFound;
import org.globsframework.utils.exceptions.TooManyItems;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static com.mongodb.client.model.Projections.include;

public class MongoSelectQuery implements SelectQuery {
    public static final Logger LOGGER = LoggerFactory.getLogger(MongoSelectQuery.class);
    private final MongoCollection<Document> collection;
    private final Map<Field, Accessor> fieldsAndAccessor;
    private final Ref<Document> currentDoc;
    private final GlobType globType;
    private final String request;
    private String lastFullRequest;
    private SqlService sqlService;
    private Constraint constraint;
    private final List<MongoSelectBuilder.Order> orders;
    private final int top;

    public MongoSelectQuery(MongoCollection<Document> collection, Map<Field, Accessor> fieldsAndAccessor,
                            Ref<Document> currentDoc, GlobType globType, SqlService sqlService, Constraint constraint,
                            List<MongoSelectBuilder.Order> orders, int top) {
        this.collection = collection;
        this.fieldsAndAccessor = fieldsAndAccessor;
        this.currentDoc = currentDoc;
        this.globType = globType;
        request = "select on " + globType.getName();
        this.sqlService = sqlService;
        this.constraint = constraint;
        this.orders = orders;
        this.top = top;
    }

    public Stream<?> executeAsStream() {
        DocumentsIterator iterator = getDocumentsIterator();
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(iterator, 0), false);
    }

    public Stream<Glob> executeAsGlobStream() {
        DocumentsIterator iterator = getDocumentsIterator();
        AccessorGlobBuilder globBuilder = AccessorGlobBuilder.init(fieldsAndAccessor.keySet(), fieldsAndAccessor::get);
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<Glob>() {
            public boolean hasNext() {
                return iterator.hasNext();
            }

            public Glob next() {
                iterator.next();
                return globBuilder.getGlob();
            }
        }, 0), false);
    }

    private DocumentsIterator getDocumentsIterator() {
        Bson filter;
        if (constraint != null) {
            MongoConstraintVisitor constraintVisitor = new MongoConstraintVisitor(sqlService);
            constraint.visit(constraintVisitor);
            filter = constraintVisitor.filter;
        } else {
            filter = new Document();
        }

        lastFullRequest = request + " where " + filter.toBsonDocument(BsonDocument.class, collection.getCodecRegistry());

        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Request filter : " + lastFullRequest);
        }
        Bson include = include(fieldsAndAccessor.keySet()
                .stream()
                .map(sqlService::getColumnName).collect(Collectors.toList()));
        FindIterable<Document> findIterable = collection.find()
                .filter(filter);
        if (!orders.isEmpty()) {
            List<Bson> bsonOrders = new ArrayList<>();
            for (MongoSelectBuilder.Order order : orders) {
                if (order.asc) {
                    bsonOrders.add(Sorts.ascending(sqlService.getColumnName(order.field)));
                } else {
                    bsonOrders.add(Sorts.descending(sqlService.getColumnName(order.field)));
                }
            }
            findIterable.sort(Sorts.orderBy(bsonOrders));
        }
        if (top != -1) {
            findIterable.limit(top);
        }
        MongoCursor<Document> iterator = findIterable
                .projection(include)
                .iterator();
        return new DocumentsIterator(currentDoc, iterator, lastFullRequest);
    }

    public DbStream execute() {
        DocumentsIterator iterator = getDocumentsIterator();
        return new DbStream() {
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
                iterator.close();
            }
        };
    }

    public GlobList executeAsGlobs() {
        DbStream dbStream = execute();
        AccessorGlobBuilder accessorGlobsBuilder = AccessorGlobBuilder.init(dbStream);
        GlobList result = new GlobList();
        while (dbStream.next()) {
            result.add(accessorGlobsBuilder.getGlob());
        }
        dbStream.close();
        return result;
    }

    public void close() {
    }

    private static class DocumentsIterator implements Iterator<Object> {
        private Ref<Document> currentDoc;
        private MongoCursor<Document> iterator;
        private String lastFullRequest;

        public DocumentsIterator(Ref<Document> currentDoc, MongoCursor<Document> iterator, String lastFullRequest) {
            this.currentDoc = currentDoc;
            this.iterator = iterator;
            this.lastFullRequest = lastFullRequest;
        }

        public boolean hasNext() {
            return iterator.hasNext();
        }

        public Object next() {
            Document next = iterator.next();
            currentDoc.set(next);
            return next;
        }

        public void close() {
            if (iterator.hasNext()) {
                LOGGER.warn("All data not read : for " + lastFullRequest);
            }
        }
    }

}
