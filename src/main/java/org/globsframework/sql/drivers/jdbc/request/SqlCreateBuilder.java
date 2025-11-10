package org.globsframework.sql.drivers.jdbc.request;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.*;
import org.globsframework.core.model.Glob;
import org.globsframework.core.streams.accessors.*;
import org.globsframework.core.streams.accessors.utils.*;
import org.globsframework.core.utils.collections.Pair;
import org.globsframework.sql.BatchSqlRequest;
import org.globsframework.sql.CreateBuilder;
import org.globsframework.sql.SqlRequest;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.drivers.jdbc.BlobUpdater;
import org.globsframework.sql.drivers.jdbc.JdbcConnection;
import org.globsframework.sql.drivers.jdbc.SqlCreateRequest;

import java.sql.Connection;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SqlCreateBuilder implements CreateBuilder {
    private Connection connection;
    private GlobType globType;
    private SqlService sqlService;
    private BlobUpdater blobUpdater;
    private JdbcConnection jdbcConnection;
    private List<Pair<Field, Accessor>> fields = new ArrayList<Pair<Field, Accessor>>();
    private Set<Field> fieldSet = new HashSet<>();
    protected DelegateGeneratedKeyAccessor generatedKeyAccessor;

    public SqlCreateBuilder(Connection connection, GlobType globType, SqlService sqlService,
                            BlobUpdater blobUpdater, JdbcConnection jdbcConnection) {
        this.connection = connection;
        this.globType = globType;
        this.sqlService = sqlService;
        this.blobUpdater = blobUpdater;
        this.jdbcConnection = jdbcConnection;
    }

    public CreateBuilder setObject(Field field, Accessor accessor) {
        fields.add(new Pair<>(field, accessor));
        if (!fieldSet.add(field)) {
            throw new RuntimeException("Field already registered");
        }
        return this;
    }

    public CreateBuilder setObject(Field field, final Object value) {
        field.safeAccept(new FieldVisitor.AbstractWithErrorVisitor() {
            public void visitInteger(IntegerField field) {
                setObject(field, new ValueIntegerAccessor((Integer) value));
            }

            public void visitLong(LongField field) {
                setObject(field, new ValueLongAccessor((Long) value));
            }

            public void visitDouble(DoubleField field) {
                setObject(field, new ValueDoubleAccessor((Double) value));
            }

            public void visitString(StringField field) {
                setObject(field, new ValueStringAccessor((String) value));
            }

            public void visitDate(DateField field) throws Exception {
                set(field, new ValueDateAccessor((LocalDate) value));
            }

            public void visitDateTime(DateTimeField field) throws Exception {
                set(field, new ValueDateTimeAccessor((ZonedDateTime) value));
            }

            public void visitStringArray(StringArrayField field) throws Exception {
                setObject(field, new ValueStringArrayAccessor((String[]) value));
            }

            public void visitBoolean(BooleanField field) {
                setObject(field, new ValueBooleanAccessor((Boolean) value));
            }

            public void visitGlob(GlobField field) throws Exception {
                setObject(field, new ValueGlobAccessor((Glob) value));
            }

            public void visitGlobArray(GlobArrayField field) throws Exception {
                setObject(field, new ValueGlobsAccessor((Glob[]) value));
            }

            public void visitLongArray(LongArrayField field) throws Exception {
                setObject(field, new ValueLongArrayAccessor((long[]) value));
            }

            public void visitUnionGlob(GlobUnionField field) throws Exception {
                setObject(field, new ValueGlobAccessor((Glob) value));
            }

            public void visitUnionGlobArray(GlobArrayUnionField field) throws Exception {
                setObject(field, new ValueGlobsAccessor((Glob[]) value));
            }

            public void visitBytes(BytesField field) {
                setObject(field, new ValueBytesAccessor((byte[]) value));
            }

        });
        return this;
    }

    public CreateBuilder set(IntegerField field, IntegerAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(LongField field, LongAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(StringField field, StringAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(StringArrayField field, StringArrayAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(DoubleField field, DoubleAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(BooleanField field, BooleanAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(BytesField field, BytesAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(BytesField field, byte[] values) {
        return setObject(field, new ValueBytesAccessor(values));
    }

    public CreateBuilder set(StringField field, String value) {
        return setObject(field, new ValueStringAccessor(value));
    }

    public CreateBuilder set(StringArrayField field, String[] value) {
        return setObject(field, new ValueStringArrayAccessor(value));
    }

    public CreateBuilder set(LongField field, Long value) {
        return setObject(field, new ValueLongAccessor(value));
    }

    public CreateBuilder set(LongArrayField field, long[] value) {
        return setObject(field, new ValueLongArrayAccessor(value));
    }

    public CreateBuilder set(DoubleField field, Double value) {
        return setObject(field, new ValueDoubleAccessor(value));
    }

    public CreateBuilder set(BooleanField field, Boolean value) {
        return setObject(field, new ValueBooleanAccessor(value));
    }

    public CreateBuilder set(DateTimeField field, ZonedDateTime value) {
        return setObject(field, new ValueDateTimeAccessor(value));
    }

    public CreateBuilder set(DateField field, LocalDate value) {
        return setObject(field, new ValueDateAccessor(value));
    }

    public CreateBuilder set(GlobField field, Glob value) {
        return setObject(field, new ValueGlobAccessor(value));
    }

    public CreateBuilder set(GlobArrayField field, Glob[] values) {
        return setObject(field, new ValueGlobsAccessor(values));
    }

    public CreateBuilder set(GlobUnionField field, Glob value) {
        return setObject(field, new ValueGlobAccessor(value));
    }

    public CreateBuilder set(GlobArrayUnionField field, Glob[] values) {
        return setObject(field, new ValueGlobsAccessor(values));
    }

    public CreateBuilder set(GlobField field, GlobAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(GlobArrayField field, GlobsAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(GlobUnionField field, GlobAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(GlobArrayUnionField field, GlobsAccessor accessor) {
        return setObject(field, accessor);
    }


    public CreateBuilder set(IntegerField field, Integer value) {
        return setObject(field, new ValueIntegerAccessor(value));
    }

    public CreateBuilder set(DateTimeField field, DateTimeAccessor accessor) {
        return setObject(field, accessor);
    }

    public CreateBuilder set(DateField field, DateAccessor accessor) {
        return setObject(field, accessor);
    }

    public Accessor getKeyGeneratedAccessor(Field field) {
        if (generatedKeyAccessor == null) {
            generatedKeyAccessor = new DelegateGeneratedKeyAccessor();
        }
        return field.safeAccept(new AllocateKeyGeneratedAccessor(generatedKeyAccessor)).generatedKeyAccessor;
    }

    public SqlRequest getRequest() {
        return new SqlCreateRequest(fields, generatedKeyAccessor, connection, globType, sqlService, blobUpdater, jdbcConnection);
    }

    public BatchSqlRequest getBulkRequest() {
        return new SqlCreateRequest(fields, generatedKeyAccessor, connection, globType, sqlService, blobUpdater, jdbcConnection);
    }

    private static class DelegateGeneratedKeyAccessor implements GeneratedKeyAccessor {
        List<GeneratedKeyAccessor> keyAccessors = new ArrayList<>();

        public void setResult(ResultSet generatedKeys, SqlService sqlService) {
            for (GeneratedKeyAccessor keyAccessor : keyAccessors) {
                keyAccessor.setResult(generatedKeys, sqlService);
            }
        }

        public void reset() {
            for (GeneratedKeyAccessor keyAccessor : keyAccessors) {
                keyAccessor.reset();
            }
        }

        public void add(GeneratedKeyAccessor generatedKeyAccessor1) {
            keyAccessors.add(generatedKeyAccessor1);
        }
    }

    private static class AllocateKeyGeneratedAccessor extends FieldVisitor.AbstractWithErrorVisitor {
        private final DelegateGeneratedKeyAccessor delegateGeneratedKeyAccessor;
        private Accessor generatedKeyAccessor;

        public AllocateKeyGeneratedAccessor(DelegateGeneratedKeyAccessor generatedKeyAccessor) {
            delegateGeneratedKeyAccessor = generatedKeyAccessor;
        }

        public void visitInteger(IntegerField field) throws Exception {
            IntegerGeneratedKeyAccessor generatedKeyAccessor1 = new IntegerGeneratedKeyAccessor(field);
            delegateGeneratedKeyAccessor.add(generatedKeyAccessor1);
            generatedKeyAccessor = generatedKeyAccessor1;
        }

        public void visitString(StringField field) throws Exception {
            StringGeneratedKeyAccessor generatedKeyAccessor1 = new StringGeneratedKeyAccessor(field);
            delegateGeneratedKeyAccessor.add(generatedKeyAccessor1);
            generatedKeyAccessor = generatedKeyAccessor1;
        }

        public void visitLong(LongField field) throws Exception {
            LongGeneratedKeyAccessor generatedKeyAccessor1 = new LongGeneratedKeyAccessor(field);
            delegateGeneratedKeyAccessor.add(generatedKeyAccessor1);
            generatedKeyAccessor = generatedKeyAccessor1;
        }
    }
}
