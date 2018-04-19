package org.globsframework.sqlstreams.drivers.mongodb;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Updates;
import org.bson.BsonReader;
import org.bson.BsonType;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.Glob;
import org.globsframework.model.MutableGlob;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.annotations.IsDbKey;
import org.globsframework.sqlstreams.drivers.mongodb.accessor.KeyStringMongoAccessor;
import org.globsframework.sqlstreams.utils.AbstractSqlService;
import org.globsframework.streams.accessors.Accessor;
import org.globsframework.streams.accessors.GlobAccessor;
import org.globsframework.utils.Ref;

import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;

import static org.globsframework.sqlstreams.drivers.mongodb.MongoUtils.getDbName;

public class MongoDbService extends AbstractSqlService {
    public static final String ID_FIELD_NAME = "_id";
    private final MongoDatabase database;
    private final Executor executor = Executors.newCachedThreadPool();
    private final UpdateAdapterFactory updateAdapterFactory;
    private ConcurrentHashMap<Field, UpdateAdapter> adapter = new ConcurrentHashMap<>();
    private final FieldReaderVisitor fieldReaderVisitor = new FieldReaderVisitor(this);

    public MongoDbService(MongoDatabase database) {
        this(database, new DefaultUpdateAdapterFactory());
    }

    public MongoDbService(MongoDatabase database, UpdateAdapterFactory updateAdapterFactory) {
        this.database = database;
        this.updateAdapterFactory = updateAdapterFactory;
    }

    public SqlConnection getDb() {
        return new MongoDbConnection(database, this);
    }

    public String getColumnName(Field field) {
        return updateAdapterFactory.getMongoFieldName(field);
    }


    public String getRootName(Field field) {
        return updateAdapterFactory.getRootName(field);
    }

    public Executor getExecutor() {
        return executor;
    }

    public UpdateAdapter getAdapter(Field field) {
        return adapter.computeIfAbsent(field, updateAdapterFactory::get);
    }

    interface UpdateAdapterFactory {

        String getMongoFieldName(Field field);

        String getRootName(Field field);

        UpdateAdapter get(Field field);
    }

    static public class DefaultUpdateAdapterFactory implements UpdateAdapterFactory {

        public String getMongoFieldName(Field field) {
            return getDbName(field);
        }

        public String getRootName(Field field) {
            return getMongoFieldName(field);
        }

        public UpdateAdapter get(Field field) {
            if (field.hasAnnotation(IsDbKey.KEY) || (field.isKeyField() && field.getGlobType().getKeyFields().length == 1 && field.getGlobType().findFieldWithAnnotation(IsDbKey.KEY) == null)) {
                return new IdUpdateAdapter(field);
            } else {
                return new DefaultUpdater(field, this);
            }
        }

        private static class IdUpdateAdapter implements UpdateAdapter {
            private final Field field;

            public IdUpdateAdapter(Field field) {
                this.field = field;
            }

            public void create(Object value, Document document) {
                document.put(ID_FIELD_NAME, new ObjectId((String) value));
            }

            public Bson update(Object value) {
                throw new RuntimeException("Call to update on id filed not expected for " + field.getFullName());
            }

            public Accessor getAccessor(Ref<Document> currentDoc) {
                return new KeyStringMongoAccessor(ID_FIELD_NAME, currentDoc);
            }

            public Object get(Document document) {
                return null;
            }
        }
    }


    public interface UpdateAdapter {
        void create(Object value, Document document);

        Bson update(Object value);

        Accessor getAccessor(Ref<Document> currentDoc);

        Object get(Document document);
    }

    static class DefaultUpdater implements UpdateAdapter {
        private final String name;

        public DefaultUpdater(Field field, DefaultUpdateAdapterFactory defaultUpdateAdapterFactory) {
            name = defaultUpdateAdapterFactory.getMongoFieldName(field);
        }

        public void create(Object value, Document doc) {
            doc.append(name, value);
        }

        public Bson update(Object value) {
            return Updates.set(name, value);
        }

        public Accessor getAccessor(Ref<Document> currentDoc) {
            // default is ok.
            return null;
        }

        public Object get(Document document) {
            return null;
        }
    }

    static class GlobUpdater implements UpdateAdapter {
        private final String name;
        private MongoDbService mongoDbService;
        private GlobField field;

        public GlobUpdater(Field field, MongoDbService mongoDbService) {
            this.field = (GlobField) field;
            name = mongoDbService.getColumnName(field);
            this.mongoDbService = mongoDbService;
        }

        public void create(Object value, Document document) {
            document.put(name, mongoDbService.toDocument((Glob) value));
        }

        public Bson update(Object value) {
            return Updates.set(name, mongoDbService.toDocument((Glob) value));
        }

        public Accessor getAccessor(Ref<Document> currentDoc) {
            return new GlobAccessor() {
                public Glob getGlob() {
                    Document document = (Document) currentDoc.get().get(name);
                    return document != null ? mongoDbService.fromDocument(field.getType(), document) : null;
                }

                public Object getObjectValue() {
                    return getGlob();
                }
            };
        }

        public Object get(Document document) {
            return null;
        }
    }


    public Document toDocument(Glob glob) {
        Document document = new Document();
        GlobType type = glob.getType();
        for (Field field : type.getFields()) {
            getAdapter(field).create(glob.getValue(field), document);
        }
        return document;
    }

    public Glob fromDocument(GlobType type, Document document) {
        MutableGlob instantiate = type.instantiate();
        for (Field field : type.getFields()) {
            field.safeVisit(fieldReaderVisitor, document, instantiate);
        }
        return instantiate;
    }

    static class FieldReaderVisitor extends FieldVisitorWithTwoContext.AbstractWithErrorVisitor<Document, MutableGlob> {
        final MongoDbService mongoDbService;

        FieldReaderVisitor(MongoDbService mongoDbService) {
            this.mongoDbService = mongoDbService;
        }

        public void visitInteger(IntegerField field, Document document, MutableGlob mutableGlob) {
            Object o = document.get(mongoDbService.getRootName(field));
            if (o != null) {
                if (o instanceof Number) {
                    mutableGlob.set(field, ((Number) o).intValue());
                }
                else {
                    throw new RuntimeException(field.getName() + " expect an int but is a " + o.getClass());
                }
            }
        }

        public void visitDouble(DoubleField field, Document document, MutableGlob mutableGlob) {
            Object o = document.get(mongoDbService.getRootName(field));
            if (o != null) {
                if (o instanceof Number) {
                    mutableGlob.set(field, ((Number) o).doubleValue());
                }
                else {
                    throw new RuntimeException(field.getName() + " expect a double but is a " + o.getClass());
                }
            }
        }

        public void visitString(StringField field, Document document, MutableGlob mutableGlob) {
            Object o = document.get(mongoDbService.getRootName(field));
            if (o != null) {
                mutableGlob.set(field, (String) o);
            }
        }

        public void visitBoolean(BooleanField field, Document document, MutableGlob mutableGlob) {
            Object o = document.get(mongoDbService.getRootName(field));
            if (o != null) {
                mutableGlob.set(field, (Boolean) o);
            }
        }

        public void visitLong(LongField field, Document document, MutableGlob mutableGlob) {
            Object o = document.get(mongoDbService.getRootName(field));
            if (o != null) {
                if (o instanceof Number) {
                    mutableGlob.set(field, ((Number) o).longValue());
                }
                else {
                    throw new RuntimeException(field.getName() + " expect a long but is a " + o.getClass());
                }
            }
        }

        public void visitBlob(BlobField field, Document document, MutableGlob mutableGlob) {
            Object o = document.get(mongoDbService.getRootName(field));
            if (o != null) {
                if (o instanceof String) {
                    mutableGlob.set(field, Base64.getDecoder().decode((String) o));
                }
                else {
                    throw new RuntimeException(field.getName() + " expect a String but is a " + o.getClass());
                }
            }
        }

        public void visitGlob(GlobField field, Document document, MutableGlob mutableGlob) throws Exception {
            Object o = document.get(mongoDbService.getRootName(field));
            if (o != null) {
                if (o instanceof Document) {
                    mutableGlob.set(field, mongoDbService.fromDocument(field.getType(), (Document) o));
                }
                else {
                    throw new RuntimeException(field.getName() + " expect a Document but is a " + o.getClass());
                }
            }
        }

        public void visitGlobArray(GlobArrayField field, Document document, MutableGlob mutableGlob) throws Exception {
            Object o = document.get(mongoDbService.getRootName(field));
            if (o != null) {
                if (o instanceof Document[]) {
                    Document[] documents = (Document[]) o;
                    Glob[] globs = new Glob[documents.length];
                    for (int i = 0; i < documents.length; i++) {
                        Document subDocument = documents[i];
                        globs[i] = mongoDbService.fromDocument(field.getType(), subDocument);
                    }
                    mutableGlob.set(field, globs);
                }
                else {
                    throw new RuntimeException(field.getName() + " expect a Document[] but is a " + o.getClass());
                }
            }

        }
    }

}

