package org.globsframework.sql.utils;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.*;
import org.globsframework.sql.SqlConnection;
import org.globsframework.sql.drivers.jdbc.JdbcSqlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.System.out;

public class GenerateTypeFromDb {
    private static Logger LOGGER = LoggerFactory.getLogger(GenerateTypeFromDb.class);
    private final String path;
    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final List<String> tableName;

    public GenerateTypeFromDb(String path, String jdbcUrl, String user, String password, List<String> tableName) {
        this.path = path;
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.tableName = tableName;
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            out.println("argument expected : [path (dump path/classname.json] jdbcURL user password tableName tableName2 ...");
            System.exit(1);
        }

        int count = 0;
        List<String> tableName = new ArrayList<>();
        for (String arg : args) {
            if (++count > 4) {
                tableName.add(arg);
            }
        }

        GenerateTypeFromDb extractFromDb = new GenerateTypeFromDb(args[0], args[1], args[2], args[3], tableName);
        extractFromDb.extractAllType();
    }

    private void extractAllType() throws IOException {
        // one service for every table: it owns a connection pool, and used to be built and dropped
        // once per table
        try (JdbcSqlService sqlService = new JdbcSqlService(jdbcUrl, user, password);
             SqlConnection db = sqlService.getDb()) {
            for (String id : tableName) {
                File source = new File(path, className(id) + ".java");
                try (Writer writer = new BufferedWriter(new OutputStreamWriter(
                        new FileOutputStream(source), StandardCharsets.UTF_8))) {
                    extractType(writer, db, id);
                }
                LOGGER.info(id + " extracted to " + source);
            }
        }
    }

    void extractType(Writer writer, SqlConnection db, String tableName) throws IOException {
        GlobType globType = db.extractType(tableName, Collections.emptySet());
        String className = className(tableName);
        writer.append("""
                package org.globsframework.generated;

                import org.globsframework.core.metamodel.GlobType;
                import org.globsframework.core.metamodel.GlobTypeBuilder;
                import org.globsframework.core.metamodel.GlobTypeBuilderFactory;
                import org.globsframework.core.metamodel.annotations.FieldName;
                import org.globsframework.core.metamodel.fields.*;

                """);
        writer.append("public class ").append(className).append(" {\n");
        writer.append("    public static final GlobType TYPE;\n\n");

        for (Field field : globType.getFields()) {
            writer.append("    public static final ").append(kindOf(field).fieldType())
                    .append(" ").append(identifier(field.getName())).append(";\n");
        }

        writer.append("\n    static {\n");
        writer.append("        GlobTypeBuilder builder = GlobTypeBuilderFactory.create(\"")
                .append(className).append("\");\n");
        for (Field field : globType.getFields()) {
            String identifier = identifier(field.getName());
            writer.append("        ").append(identifier).append(" = builder.")
                    .append(kindOf(field).declareMethod()).append("(\"").append(identifier).append("\"");
            // the column name is kept as an annotation whenever it is not the Java identifier itself
            if (!identifier.equals(field.getName())) {
                writer.append(", FieldName.create(\"").append(field.getName()).append("\")");
            }
            writer.append(");\n");
        }
        writer.append("        TYPE = builder.build();\n");
        writer.append("    }\n}\n");
        writer.flush();
    }

    /**
     * A table or column name is not necessarily a Java identifier — a dash, a leading digit, a space.
     * The original name is kept as a FieldName annotation, so nothing is lost.
     */
    static String identifier(String name) {
        String cleaned = name.replaceAll("[^A-Za-z0-9_]", "_");
        return cleaned.isEmpty() || Character.isDigit(cleaned.charAt(0)) ? "_" + cleaned : cleaned;
    }

    static String className(String tableName) {
        String identifier = identifier(tableName);
        return Character.toUpperCase(identifier.charAt(0)) + identifier.substring(1);
    }

    private record Kind(String fieldType, String declareMethod) {
    }

    private Kind kindOf(Field field) {
        return field.safeAccept(new FieldVisitor() {
            Kind kind;

            public void visitInteger(IntegerField field) throws Exception {
                kind = new Kind("IntegerField", "declareIntegerField");
            }

            public void visitIntegerArray(IntegerArrayField field) throws Exception {
                kind = new Kind("IntegerArrayField", "declareIntegerArrayField");
            }

            public void visitDouble(DoubleField field) throws Exception {
                kind = new Kind("DoubleField", "declareDoubleField");
            }

            public void visitDoubleArray(DoubleArrayField field) throws Exception {
                kind = new Kind("DoubleArrayField", "declareDoubleArrayField");
            }

            public void visitBigDecimal(BigDecimalField field) throws Exception {
                kind = new Kind("BigDecimalField", "declareBigDecimalField");
            }

            public void visitBigDecimalArray(BigDecimalArrayField field) throws Exception {
                kind = new Kind("BigDecimalArrayField", "declareBigDecimalArrayField");
            }

            public void visitString(StringField field) throws Exception {
                kind = new Kind("StringField", "declareStringField");
            }

            public void visitStringArray(StringArrayField field) throws Exception {
                kind = new Kind("StringArrayField", "declareStringArrayField");
            }

            public void visitBoolean(BooleanField field) throws Exception {
                kind = new Kind("BooleanField", "declareBooleanField");
            }

            public void visitBooleanArray(BooleanArrayField field) throws Exception {
                kind = new Kind("BooleanArrayField", "declareBooleanArrayField");
            }

            public void visitLong(LongField field) throws Exception {
                kind = new Kind("LongField", "declareLongField");
            }

            public void visitLongArray(LongArrayField field) throws Exception {
                kind = new Kind("LongArrayField", "declareLongArrayField");
            }

            public void visitDate(DateField field) throws Exception {
                kind = new Kind("DateField", "declareDateField");
            }

            public void visitDateTime(DateTimeField field) throws Exception {
                kind = new Kind("DateTimeField", "declareDateTimeField");
            }

            public void visitBytes(BytesField field) throws Exception {
                kind = new Kind("BytesField", "declareBytesField");
            }

            public void visitGlob(GlobField<?> field) throws Exception {
                throw new UnsupportedOperationException("GlobField cannot be generated: JDBC metadata gives no target type");
            }

            public void visitGlobArray(GlobArrayField<?> field) throws Exception {
                throw new UnsupportedOperationException("GlobArrayField cannot be generated: JDBC metadata gives no target type");
            }

            public void visitUnionGlob(GlobUnionField field) throws Exception {
                throw new UnsupportedOperationException("GlobUnionField cannot be generated: JDBC metadata gives no target type");
            }

            public void visitUnionGlobArray(GlobArrayUnionField field) throws Exception {
                throw new UnsupportedOperationException("GlobArrayUnionField cannot be generated: JDBC metadata gives no target type");
            }
        }).kind;
    }
}
