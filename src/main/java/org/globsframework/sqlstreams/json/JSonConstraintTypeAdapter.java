package org.globsframework.sqlstreams.json;

import com.google.gson.*;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import org.globsframework.json.GSonUtils;
import org.globsframework.json.GSonVisitor;
import org.globsframework.json.JsonDateTimeFormatType;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobTypeResolver;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.Glob;
import org.globsframework.sqlstreams.constraints.*;
import org.globsframework.sqlstreams.constraints.impl.*;
import org.globsframework.utils.Ref;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JSonConstraintTypeAdapter extends TypeAdapter<Constraint> {
    public static final String IN = "in";
    public static final String NOT_IN = "notIn";
    public static final String OR = "or";
    public static final String AND = "and";
    public static final String LEFT = "left";
    public static final String RIGHT = "right";
    public static final String EQUAL = "equal";
    public static final String CONTAINS = "contains";
    public static final String CONTAINS_WITH_IGNORE_CASE = "containsIgnoreCase";
    public static final String START_WITH = "start";
    public static final String START_WITH_IGNORE_CASE = "startIgnoreCase";
    public static final String NOT_CONTAINS = "notContains";
    public static final String NOT_CONTAINS_WITH_IGNORE_CASE = "notContainsIgnoreCase";
    public static final String START_NOT_CONTAINS = "notStart";
    public static final String CASE_SENSITIVE_REGEXP_MATCHES = "caseSensitiveRegex";
    public static final String CASE_INSENSITIVE_REGEXP_MATCHES = "caseInsensitiveRegex";
    public static final String NOT_CASE_SENSITIVE_REGEXP_MATCHES = "notCaseSensitiveRegex";
    public static final String NOT_CASE_INSENSITIVE_REGEXP_MATCHES = "notCaseInsensitiveRegex";
    public static final String START_NOT_CONTAINS_WITH_IGNORE_CASE = "notStartIgnoreCase";
    public static final String IS_NULL = "isNull";
    public static final String IS_NOT_NULL = "isNotNull";
    public static final String NOT_EQUAL = "notEqual";
    public static final String LESS_THAN = "lessThan";
    public static final String STRICTLY_LESS_THAN = "strictlyLessThan";
    public static final String GREATER_THAN = "greaterThan";
    public static final String STRICTLY_GREATER_THAN = "strictlyGreaterThan";
    public static final String VALUE = "value";
    public static final String VALUES = "values";
    public static final String FIELD = "field";
    public static final String TYPE = "type";
    public static final String FIELD_NAME = "name";
    private final FieldResolver resolver;

    public interface FieldResolver {
        Field getField(String type, String name);
    }

    public static class DefaultFieldResolver implements FieldResolver {
        GlobTypeResolver globTypeResolver;

        public DefaultFieldResolver(GlobTypeResolver globTypeResolver) {
            this.globTypeResolver = globTypeResolver;
        }

        public Field getField(String type, String name) {
            return globTypeResolver.getType(type).getField(name);
        }
    }

    public static Constraint decode(String str, FieldResolver fieldResolver) {
        JSonConstraintTypeAdapter jSonConstraintTypeAdapter =
                new JSonConstraintTypeAdapter(fieldResolver);
        return jSonConstraintTypeAdapter.read(new JsonReader(new StringReader(str)));
    }

    public static Constraint decode(Reader reader, FieldResolver fieldResolver) {
        JSonConstraintTypeAdapter jSonConstraintTypeAdapter =
                new JSonConstraintTypeAdapter(fieldResolver);
        return jSonConstraintTypeAdapter.read(new JsonReader(reader));
    }

    static public String encode(Constraint constraint) {
        StringBuilder stringBuilder = new StringBuilder();
        Writer out = new GSonUtils.StringWriterToBuilder(stringBuilder);
        encode(constraint, out);
        return stringBuilder.toString();
    }

    private static void encode(Constraint constraint, Writer out) {
        JSonConstraintTypeAdapter jSonConstraintTypeAdapter =
                new JSonConstraintTypeAdapter(new DefaultFieldResolver(GlobTypeResolver.ERROR));
        try {
            jSonConstraintTypeAdapter.write(new JsonWriter(out), constraint);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    static public GsonBuilder createBuilder(FieldResolver globTypeResolver) {
        return new GsonBuilder()
                .registerTypeHierarchyAdapter(Constraint.class, new JSonConstraintTypeAdapter(globTypeResolver));
    }

    static public GsonBuilder register(GsonBuilder gsonBuilder, GlobTypeResolver globTypeResolver) {
        return gsonBuilder.registerTypeHierarchyAdapter(Constraint.class, new JSonConstraintTypeAdapter(new DefaultFieldResolver(globTypeResolver)));
    }

    static public GsonBuilder register(GsonBuilder gsonBuilder, FieldResolver fieldResolver) {
        return gsonBuilder.registerTypeHierarchyAdapter(Constraint.class, new JSonConstraintTypeAdapter(fieldResolver));
    }

    public static Gson create(GlobTypeResolver globTypeResolver) {
        return createBuilder(new DefaultFieldResolver(globTypeResolver)).create();
    }

    public static Gson create(FieldResolver fieldResolver) {
        return createBuilder(fieldResolver).create();
    }

    public JSonConstraintTypeAdapter(FieldResolver resolver) {
        this.resolver = resolver;
    }

    public void write(JsonWriter out, Constraint constraint) throws IOException {
        if (constraint == null) {
            out.nullValue();
            return;
        }
        out.beginObject();
        constraint.visit(new JSonConstraintVisitor(out));
        out.endObject();
    }

    public Constraint read(JsonReader in) {
        JsonParser jsonParser = new JsonParser();
        JsonElement element = jsonParser.parse(in);
        if (element.isJsonNull()) {
            return null;
        }
        return readConstraint((JsonObject) element);
    }

    private Constraint readConstraint(JsonObject object) {
        Constraint constraint = null;
        Set<Map.Entry<String, JsonElement>> entries = object.entrySet();
        if (entries.size() != 1) {
            throw new RuntimeException("Only one element expected got " + entries);
        }
        Map.Entry<String, JsonElement> entry = entries.iterator().next();

        switch (entry.getKey()) {
            case EQUAL: {
                Ref<Operand> leftOp = new Ref<>();
                Ref<Operand> rightOp = new Ref<>();
                findField((JsonObject) entry.getValue(), leftOp, rightOp);
                return new EqualConstraint(leftOp.get(), rightOp.get());
            }
            case CONTAINS: {
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonElement jsonElement = in.get(VALUE);
                return new ContainsConstraint(field, jsonElement.getAsString(), false, true, false);
            }
            case CONTAINS_WITH_IGNORE_CASE: {
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonElement jsonElement = in.get(VALUE);
                return new ContainsConstraint(field, jsonElement.getAsString(), false, true, true);
            }
            case START_WITH: {
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonElement jsonElement = in.get(VALUE);
                return new ContainsConstraint(field, jsonElement.getAsString(), true, true, false);
            }
            case START_WITH_IGNORE_CASE: {
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonElement jsonElement = in.get(VALUE);
                return new ContainsConstraint(field, jsonElement.getAsString(), true, true, true);
            }
            case NOT_CONTAINS: {
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonElement jsonElement = in.get(VALUE);
                return new ContainsConstraint(field, jsonElement.getAsString(), false, false, false);
            }
            case NOT_CONTAINS_WITH_IGNORE_CASE: {
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonElement jsonElement = in.get(VALUE);
                return new ContainsConstraint(field, jsonElement.getAsString(), false, false, true);
            }
            case START_NOT_CONTAINS: {
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonElement jsonElement = in.get(VALUE);
                return new ContainsConstraint(field, jsonElement.getAsString(), true, false, false);
            }
            case START_NOT_CONTAINS_WITH_IGNORE_CASE: {
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonElement jsonElement = in.get(VALUE);
                return new ContainsConstraint(field, jsonElement.getAsString(), true, false, true);
            }
            case NOT_EQUAL: {
                Ref<Operand> leftOp = new Ref<>();
                Ref<Operand> rightOp = new Ref<>();
                findField((JsonObject) entry.getValue(), leftOp, rightOp);
                return new NotEqualConstraint(leftOp.get(), rightOp.get());
            }
            case IS_NULL: {
                Field field = readField(((JsonObject) entry.getValue()));
                return new NullOrNotConstraint(field, true);
            }
            case IS_NOT_NULL: {
                Field field = readField(((JsonObject) entry.getValue()));
                return new NullOrNotConstraint(field, false);
            }
            case AND: {
                JsonArray array = entry.getValue().getAsJsonArray();
                for (JsonElement jsonElement : array) {
                    constraint = Constraints.and(constraint, readConstraint((JsonObject) jsonElement));
                }
                return constraint;
            }
            case OR: {
                JsonArray array = entry.getValue().getAsJsonArray();
                for (JsonElement jsonElement : array) {
                    constraint = Constraints.or(constraint, readConstraint((JsonObject) jsonElement));
                }
                return constraint;
            }
            case IN: {
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonArray asJsonArray = in.getAsJsonArray(VALUES);
                JsonFieldValueReaderVisitor visitor = new JsonFieldValueReaderVisitor();
                Set<Object> values = new HashSet<>();
                for (JsonElement jsonElement : asJsonArray) {
                    values.add(field.safeVisit(visitor, jsonElement).value);
                }
                return Constraints.in(field, values);
            }
            case NOT_IN: {
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonArray asJsonArray = in.getAsJsonArray(VALUES);
                JsonFieldValueReaderVisitor visitor = new JsonFieldValueReaderVisitor();
                Set<Object> values = new HashSet<>();
                for (JsonElement jsonElement : asJsonArray) {
                    values.add(field.safeVisit(visitor, jsonElement).value);
                }
                return Constraints.notIn(field, values);
            }
            case LESS_THAN: {
                Ref<Operand> leftOp = new Ref<>();
                Ref<Operand> rightOp = new Ref<>();
                findField((JsonObject) entry.getValue(), leftOp, rightOp);
                return new LessThanConstraint(leftOp.get(), rightOp.get());
            }
            case STRICTLY_LESS_THAN: {
                Ref<Operand> leftOp = new Ref<>();
                Ref<Operand> rightOp = new Ref<>();
                findField((JsonObject) entry.getValue(), leftOp, rightOp);
                return new StrictlyLesserThanConstraint(leftOp.get(), rightOp.get());
            }
            case GREATER_THAN: {
                Ref<Operand> leftOp = new Ref<>();
                Ref<Operand> rightOp = new Ref<>();
                findField((JsonObject) entry.getValue(), leftOp, rightOp);
                return new BiggerThanConstraint(leftOp.get(), rightOp.get());
            }
            case STRICTLY_GREATER_THAN: {
                Ref<Operand> leftOp = new Ref<>();
                Ref<Operand> rightOp = new Ref<>();
                findField((JsonObject) entry.getValue(), leftOp, rightOp);
                return new StrictlyBiggerThanConstraint(leftOp.get(), rightOp.get());
            }
            case CASE_SENSITIVE_REGEXP_MATCHES:{
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonElement jsonElement = in.get(VALUE);
                return new RegularExpressionConstraint(field, jsonElement.getAsString(), false, false);
            }
            case CASE_INSENSITIVE_REGEXP_MATCHES: {
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonElement jsonElement = in.get(VALUE);
                return new RegularExpressionConstraint(field, jsonElement.getAsString(), true, false);
            }
            case NOT_CASE_SENSITIVE_REGEXP_MATCHES:{
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonElement jsonElement = in.get(VALUE);
                return new RegularExpressionConstraint(field, jsonElement.getAsString(), false, true);
            }
            case NOT_CASE_INSENSITIVE_REGEXP_MATCHES: {
                JsonObject in = (JsonObject) entry.getValue();
                Field field = readField(in);
                JsonElement jsonElement = in.get(VALUE);
                return new RegularExpressionConstraint(field, jsonElement.getAsString(), true, true);
            }
        }
        throw new RuntimeException(entry.getKey() + " not managed");
    }

    private void findField(JsonObject object, Ref<Operand> leftOp, Ref<Operand> rightOp) {
        JsonObject left = object.getAsJsonObject(LEFT);
        JsonObject right = object.getAsJsonObject(RIGHT);
        JsonObject opposite = null;
        Ref<Operand> oppositeRef = null;
        JsonElement fieldObj = left.get(FIELD);
        Field field;
        if (fieldObj != null) {
            field = readField(left);
            leftOp.set(new FieldOperand(field));
            opposite = right;
            oppositeRef = rightOp;
        } else {
            fieldObj = right.get(FIELD);
            if (fieldObj != null) {
                field = readField(right);
                rightOp.set(new FieldOperand(field));
                opposite = left;
                oppositeRef = leftOp;
            } else {
                throw new RuntimeException("At least one of left or right should be a field type");
            }
        }
        if (opposite.get(VALUE) != null) {
            oppositeRef.set(new ValueOperand(field, readValue(field, opposite)));
        } else {
            oppositeRef.set(new FieldOperand(readField(opposite)));
        }
    }

    private Object readValue(Field field, JsonObject jsonElement) {
        JsonElement value = jsonElement.get(VALUE);
        if (value == null) {
            throw new RuntimeException("A value is expected ");
        }
        return field.safeVisit(new JsonFieldValueReaderVisitor(), value).value;
    }

    private Field readField(JsonObject object) {
        JsonObject field = object.getAsJsonObject(FIELD);
        JsonElement type = field.get(TYPE);
        if (type == null) {
            throw new RuntimeException("A type is expected");
        }
        JsonElement name = field.get(FIELD_NAME);
        if (name != null) {
            return resolver.getField(type.getAsString(), name.getAsString());
        }
        throw new RuntimeException("A field is expected ");
    }

    private static class JSonConstraintVisitor extends FieldValueVisitor.AbstractWithErrorVisitor implements ConstraintVisitor, OperandVisitor {
        private JsonWriter jsonWriter;

        public JSonConstraintVisitor(JsonWriter jsonWriter) {
            this.jsonWriter = jsonWriter;
        }

        public void visitEqual(EqualConstraint constraint) {
            try {
                jsonWriter.name(EQUAL);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void visitBinary(BinaryOperandConstraint constraint) throws IOException {
            jsonWriter.beginObject();
            jsonWriter.name(LEFT);
            jsonWriter.beginObject();
            constraint.getLeftOperand()
                    .visitOperand(this);
            jsonWriter.endObject();

            jsonWriter.name(RIGHT);
            jsonWriter.beginObject();
            constraint.getRightOperand()
                    .visitOperand(this);
            jsonWriter.endObject();

            jsonWriter.endObject();
        }

        public void visitNotEqual(NotEqualConstraint constraint) {
            try {
                jsonWriter.name(NOT_EQUAL);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        private void visitBinary(BinaryConstraint constraint) throws IOException {
            jsonWriter.beginArray();
            jsonWriter.beginObject();
            constraint.getLeftConstraint()
                    .visit(this);
            jsonWriter.endObject();

            jsonWriter.beginObject();
            constraint.getRightConstraint()
                    .visit(this);
            jsonWriter.endObject();

            jsonWriter.endArray();
        }

        public void visitAnd(AndConstraint constraint) {
            try {
                jsonWriter.name(AND);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitOr(OrConstraint constraint) {
            try {
                jsonWriter.name(OR);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitLessThan(LessThanConstraint constraint) {
            try {
                jsonWriter.name(LESS_THAN);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitBiggerThan(BiggerThanConstraint constraint) {
            try {
                jsonWriter.name(GREATER_THAN);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitStrictlyBiggerThan(StrictlyBiggerThanConstraint constraint) {
            try {
                jsonWriter.name(STRICTLY_GREATER_THAN);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitStrictlyLesserThan(StrictlyLesserThanConstraint constraint) {
            try {
                jsonWriter.name(STRICTLY_LESS_THAN);
                visitBinary(constraint);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitIn(InConstraint constraint) {
            try {
                jsonWriter.name(IN);
                jsonWriter.beginObject();
                visitFieldOperand(constraint.getField());
                jsonWriter.name(VALUES)
                        .beginArray();
                for (Object o : constraint.getValues()) {
                    constraint.getField().safeVisit(this, o);
                }
                jsonWriter.endArray();
                jsonWriter.endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitIsOrNotNull(NullOrNotConstraint constraint) {
            try {
                if (constraint.checkNull()) {
                    jsonWriter.name(IS_NULL);
                } else {
                    jsonWriter.name(IS_NOT_NULL);
                }
                jsonWriter.beginObject();
                visitFieldOperand(constraint.getField());
                jsonWriter.endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitNotIn(NotInConstraint constraint) {
            try {
                jsonWriter.name(NOT_IN);
                jsonWriter.beginObject();
                visitFieldOperand(constraint.getField());
                jsonWriter.name(VALUES)
                        .beginArray();
                for (Object o : constraint.getValues()) {
                    constraint.getField().safeVisit(this, o);
                }
                jsonWriter.endArray();
                jsonWriter.endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitContains(Field field, String value, boolean contains, boolean startWith, boolean ignoreCase) {
            try {
                if (startWith) {
                    if (ignoreCase) {
                        jsonWriter.name(contains ? START_WITH_IGNORE_CASE : START_NOT_CONTAINS_WITH_IGNORE_CASE);
                    }
                    else {
                        jsonWriter.name(contains ? START_WITH : START_NOT_CONTAINS);
                    }
                } else {
                    if (ignoreCase) {
                        jsonWriter.name(contains ? CONTAINS_WITH_IGNORE_CASE : NOT_CONTAINS_WITH_IGNORE_CASE);
                    }
                    else {
                        jsonWriter.name(contains ? CONTAINS : NOT_CONTAINS);
                    }
                }
                jsonWriter.beginObject();
                visitFieldOperand(field);
                jsonWriter.name(VALUE).value(value);
                jsonWriter.endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        }

        public void visitRegularExpression(Field field, String value, boolean caseSensitive, boolean not) {
            try {
                if(caseSensitive) {
                    jsonWriter.name(not ? NOT_CASE_SENSITIVE_REGEXP_MATCHES : CASE_SENSITIVE_REGEXP_MATCHES);
                } else {
                    jsonWriter.name(not ? NOT_CASE_INSENSITIVE_REGEXP_MATCHES : CASE_INSENSITIVE_REGEXP_MATCHES);
                }
                jsonWriter.beginObject();
                visitFieldOperand(field);
                jsonWriter.name(VALUE).value(value);
                jsonWriter.endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        public void visitValueOperand(ValueOperand valueOperand) {
            try {
                jsonWriter.name(VALUE);
                valueOperand.getField().safeVisit(this, valueOperand.getValue());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitAccessorOperand(AccessorOperand accessorOperand) {
            try {
                jsonWriter.name(VALUE);
                accessorOperand.getField().safeVisit(this, accessorOperand.getAccessor().getObjectValue());
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitFieldOperand(Field field) {
            try {
                jsonWriter.name(FIELD);
                jsonWriter.beginObject();
                jsonWriter.name(TYPE).value(field.getGlobType().getName());
                jsonWriter.name(FIELD_NAME).value(field.getName());
                jsonWriter.endObject();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }

        public void visitInteger(IntegerField field, Integer value) throws Exception {
            if (value != null) {
                jsonWriter.value(value);
            }
        }

        public void visitDouble(DoubleField field, Double value) throws Exception {
            if (value != null) {
                jsonWriter.value(value);
            }
        }

        public void visitString(StringField field, String value) throws Exception {
            if (value != null) {
                jsonWriter.value(value);
            }
        }

        public void visitBoolean(BooleanField field, Boolean value) throws Exception {
            if (value != null) {
                jsonWriter.value(value);
            }
        }

        public void visitLong(LongField field, Long value) throws Exception {
            if (value != null) {
                jsonWriter.value(value);
            }
        }

        public void visitBlob(BlobField field, byte[] value) throws Exception {
            if (value != null) {
                jsonWriter.value(Base64.getEncoder().encodeToString(value));
            }
        }

        public void visitDate(DateField field, LocalDate value) throws Exception {
            if (value != null) {
                final DateTimeFormatter cachedDateFormatter = GSonUtils.getCachedDateFormatter(field);
                jsonWriter.value(cachedDateFormatter.format(value));
            }
        }

        public void visitDateTime(DateTimeField field, ZonedDateTime value) throws Exception {
            if (value != null) {
                final DateTimeFormatter cachedDateTimeFormatter = GSonUtils.getCachedDateTimeFormatter(field);
                jsonWriter.value(cachedDateTimeFormatter.format(value));
            }
        }
    }

    static class JsonFieldValueReaderVisitor extends FieldVisitorWithContext.AbstractWithErrorVisitor<JsonElement> {
        Object value;

        public void visitInteger(IntegerField field, JsonElement context) throws Exception {
            value = context.getAsInt();
        }

        public void visitDouble(DoubleField field, JsonElement context) throws Exception {
            value = context.getAsDouble();
        }

        public void visitString(StringField field, JsonElement context) throws Exception {
            value = context.getAsString();
        }

        public void visitBoolean(BooleanField field, JsonElement context) throws Exception {
            value = context.getAsBoolean();

        }

        public void visitLong(LongField field, JsonElement context) throws Exception {
            value = context.getAsLong();
        }

        public void visitBlob(BlobField field, JsonElement context) throws Exception {
            value = Base64.getDecoder().decode(context.getAsString());
        }

        public void visitDate(DateField field, JsonElement context) throws Exception {
            final DateTimeFormatter cachedDateFormatter = GSonUtils.getCachedDateFormatter(field);
            value = LocalDate.from(cachedDateFormatter.parse(context.getAsString()));
        }

        public void visitDateTime(DateTimeField field, JsonElement context) throws Exception {
            final DateTimeFormatter cachedDateTimeFormatter = GSonUtils.getCachedDateTimeFormatter(field);
            if (field.hasAnnotation(JsonDateTimeFormatType.UNIQUE_KEY)) {
                Glob annotation = field.getAnnotation(JsonDateTimeFormatType.UNIQUE_KEY);
                Boolean aBoolean = annotation.get(JsonDateTimeFormatType.AS_LOCAL);
                if (aBoolean) {
                    value = ZonedDateTime.of(LocalDateTime.from(cachedDateTimeFormatter.parse(context.getAsString())), ZoneId.systemDefault());
                    return;
                }
            }
            value = ZonedDateTime.from(cachedDateTimeFormatter.parse(context.getAsString()));
        }
    }
}
