package org.globsframework.sql.utils;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.*;
import org.globsframework.core.model.Glob;
import org.globsframework.core.streams.accessors.*;
import org.globsframework.sql.BatchSqlRequest;
import org.globsframework.sql.CreateBuilder;
import org.globsframework.sql.SqlConnection;
import org.globsframework.sql.SqlRequest;
import org.globsframework.sql.exceptions.SqlException;

import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MultiCreateBuilder implements CreateBuilder {
    private Map<GlobType, CreateBuilder> createBuilders;

    public MultiCreateBuilder(SqlConnection sqlConnection, Collection<GlobType> globTypes) {
        createBuilders = new HashMap<GlobType, CreateBuilder>(globTypes.size());
        for (GlobType globType : globTypes) {
            createBuilders.put(globType, sqlConnection.getCreateBuilder(globType));
        }
    }

    public CreateBuilder set(IntegerField field, Integer value) {
        createBuilders.get(field.getGlobType()).set(field, value);
        return this;
    }

    public CreateBuilder set(BlobField field, byte[] value) {
        createBuilders.get(field.getGlobType()).set(field, value);
        return this;
    }

    public CreateBuilder set(StringField field, String value) {
        createBuilders.get(field.getGlobType()).set(field, value);
        return this;
    }

    public CreateBuilder set(StringArrayField field, String[] value) {
        createBuilders.get(field.getGlobType()).set(field, value);
        return this;
    }

    public CreateBuilder set(LongField field, Long value) {
        createBuilders.get(field.getGlobType()).set(field, value);
        return this;
    }

    public CreateBuilder set(LongArrayField field, long[] value) {
        createBuilders.get(field.getGlobType()).set(field, value);
        return this;
    }

    public CreateBuilder set(DoubleField field, Double value) {
        createBuilders.get(field.getGlobType()).set(field, value);
        return this;
    }

    public CreateBuilder set(BooleanField field, Boolean value) {
        createBuilders.get(field.getGlobType()).set(field, value);
        return this;
    }

    public CreateBuilder set(DateField field, LocalDate value) {
        createBuilders.get(field.getGlobType()).set(field, value);
        return this;
    }

    public CreateBuilder set(DateTimeField field, ZonedDateTime value) {
        createBuilders.get(field.getGlobType()).set(field, value);
        return this;
    }

    public CreateBuilder set(IntegerField field, IntegerAccessor accessor) {
        createBuilders.get(field.getGlobType()).set(field, accessor);
        return this;
    }

    public CreateBuilder set(LongField field, LongAccessor accessor) {
        createBuilders.get(field.getGlobType()).set(field, accessor);
        return this;
    }

    public CreateBuilder set(StringField field, StringAccessor accessor) {
        createBuilders.get(field.getGlobType()).set(field, accessor);
        return this;
    }

    public CreateBuilder set(StringArrayField field, StringArrayAccessor accessor) {
        createBuilders.get(field.getGlobType()).set(field, accessor);
        return this;
    }

    public CreateBuilder set(DoubleField field, DoubleAccessor accessor) {
        createBuilders.get(field.getGlobType()).set(field, accessor);
        return this;
    }

    public CreateBuilder set(BooleanField field, BooleanAccessor accessor) {
        createBuilders.get(field.getGlobType()).set(field, accessor);
        return this;
    }

    public CreateBuilder set(BlobField field, BlobAccessor accessor) {
        createBuilders.get(field.getGlobType()).set(field, accessor);
        return this;
    }

    public CreateBuilder set(GlobField field, Glob value) {
        createBuilders.get(field.getGlobType()).set(field, value);
        return this;
    }

    public CreateBuilder set(GlobArrayField field, Glob[] values) {
        createBuilders.get(field.getGlobType()).set(field, values);
        return this;
    }

    public CreateBuilder set(GlobField field, GlobAccessor accessor) {
        createBuilders.get(field.getGlobType()).set(field, accessor);
        return this;
    }

    public CreateBuilder set(GlobArrayField field, GlobsAccessor accessor) {
        createBuilders.get(field.getGlobType()).set(field, accessor);
        return this;
    }

    public CreateBuilder set(GlobUnionField field, Glob value) {
        createBuilders.get(field.getGlobType()).set(field, value);
        return this;
    }

    public CreateBuilder set(GlobArrayUnionField field, Glob[] values) {
        createBuilders.get(field.getGlobType()).set(field, values);
        return this;
    }

    public CreateBuilder set(GlobUnionField field, GlobAccessor accessor) {
        createBuilders.get(field.getGlobType()).set(field, accessor);
        return this;
    }

    public CreateBuilder set(GlobArrayUnionField field, GlobsAccessor accessor) {
        createBuilders.get(field.getGlobType()).set(field, accessor);
        return this;
    }

    public CreateBuilder set(DateTimeField field, DateTimeAccessor accessor) {
        createBuilders.get(field.getGlobType()).set(field, accessor);
        return this;
    }

    public CreateBuilder set(DateField field, DateAccessor accessor) {
        createBuilders.get(field.getGlobType()).set(field, accessor);
        return this;
    }

    public CreateBuilder setObject(Field field, Accessor accessor) {
        createBuilders.get(field.getGlobType()).setObject(field, accessor);
        return this;
    }

    public CreateBuilder setObject(Field field, Object value) {
        createBuilders.get(field.getGlobType()).setObject(field, value);
        return this;
    }

    @Override
    public Accessor getKeyGeneratedAccessor(Field field) {
        throw new RuntimeException("Not implemented");
    }

    public SqlRequest getRequest() {
        return new MultiSqlRequest(createBuilders);
    }

    public BatchSqlRequest getBulkRequest() {
        return new MultiBatchSqlRequest(createBuilders);
    }

    static private class MultiSqlRequest implements SqlRequest {
        private Collection<SqlRequest> sqlRequests;

        public MultiSqlRequest(Map<GlobType, CreateBuilder> createBuilders) {
            sqlRequests = new ArrayList<>(createBuilders.size());
            for (CreateBuilder builder : createBuilders.values()) {
                sqlRequests.add(builder.getRequest());
            }
        }

        public int apply() throws SqlException {
            int result = 0;
            for (SqlRequest sqlRequest : sqlRequests) {
                result += sqlRequest.apply();
            }
            return result;
        }

        public void close() {
            for (SqlRequest request : sqlRequests) {
                request.close();
            }
        }
    }

    static private class MultiBatchSqlRequest implements BatchSqlRequest {
        private Collection<BatchSqlRequest> sqlRequests;

        public MultiBatchSqlRequest(Map<GlobType, CreateBuilder> createBuilders) {
            sqlRequests = new ArrayList<BatchSqlRequest>(createBuilders.size());
            for (CreateBuilder builder : createBuilders.values()) {
                sqlRequests.add(builder.getBulkRequest());
            }
        }

        public void addBatch() throws SqlException {
            for (BatchSqlRequest sqlRequest : sqlRequests) {
              sqlRequest.addBatch();
            }
        }

        public void close() {
            for (BatchSqlRequest request : sqlRequests) {
                request.close();
            }
        }

        public int[] applyBatch() {
            int[] result = null;
            for (BatchSqlRequest request : sqlRequests) {
                final int[] tmp = request.applyBatch();
                if (result == null) {
                    result = tmp;
                }
                else {
                    for (int i = 0; i < result.length; i++) {
                        result[i] += tmp[i];
                    }
                }
            }
            return result;
        }
    }
}
