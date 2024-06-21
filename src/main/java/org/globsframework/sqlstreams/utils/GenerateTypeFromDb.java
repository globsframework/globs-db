package org.globsframework.sqlstreams.utils;

import org.globsframework.metamodel.fields.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcSqlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
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
        {
            for (String id : tableName) {
                FileOutputStream jsonFile =
                        new FileOutputStream(new File(path, id + "Type" + ".json"));
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(jsonFile, "UTF-8"));
                extractType(writer, id);
                writer.close();
                LOGGER.info(id + " extracted");
            }
        }
    }

    private void extractType(Writer writer, String tableName) throws IOException {
        JdbcSqlService jdbcSqlService = new JdbcSqlService(jdbcUrl, user, password);
        GlobType globType = jdbcSqlService.getDb().extractType(tableName, Collections.emptySet());
        writer.append(
                "package org.globsframework.generated;\n" +
                        "\n" +
                        "import org.globsframework.metamodel.GlobType;\n" +
                        "import org.globsframework.metamodel.GlobTypeLoaderFactory;\n" +
                        "import org.globsframework.metamodel.annotations.GlobCreateFromAnnotation;\n" +
                        "import org.globsframework.metamodel.annotations.InitUniqueKey;\n" +
                        "import org.globsframework.metamodel.fields.StringField;\n" +
                        "import org.globsframework.metamodel.fields.DoubleField;\n" +
                        "import org.globsframework.metamodel.fields.StringArrayField;\n" +
                        "import org.globsframework.metamodel.fields.DoubleArrayField;\n" +
                        "import org.globsframework.metamodel.fields.LongField;\n" +
                        "import org.globsframework.metamodel.fields.LongArrayField;\n" +
                        "import org.globsframework.metamodel.fields.IntegerField;\n" +
                        "import org.globsframework.metamodel.fields.IntegerArrayField;\n" +
                        "import org.globsframework.metamodel.fields.GlobField;\n" +
                        "import org.globsframework.metamodel.fields.GlobArrayField;\n" +
                        "import org.globsframework.metamodel.fields.GlobUnionField;\n" +
                        "import org.globsframework.metamodel.fields.GlobArrayUnionField;\n" +
                        "import org.globsframework.metamodel.fields.BigDecimalField;\n" +
                        "import org.globsframework.metamodel.fields.BigDecimalArrayField;\n" +
                        "import org.globsframework.metamodel.fields.DateField;\n" +
                        "import org.globsframework.metamodel.fields.DateTimeField;\n" +
                        "import org.globsframework.metamodel.fields.BlobField;\n" +
                        "import org.globsframework.metamodel.annotations.FieldNameAnnotation;\n" +
                        "import org.globsframework.model.Key;\n" +
                        "\n" +
                        "public class " + tableName + "  {\n" +
                        "    public static GlobType TYPE;\n"
        );

        for (Field field : globType.getFields()) {
            writer.append("@FieldNameAnnotation(\"" + field.getName() + "\")\n");
            writer.append("public static " );
            writer.append(getType(field));
            writer.append(" " + field.getName() + ";\n");
        }


        writer.append(
                " static {\n" +
                        "        GlobTypeLoaderFactory.create(" + tableName + ".class, \"" + tableName + "\");\n" +
                        "}\n" +
                        "};\n" +
                        "");
        writer.flush();
    }

    private String getType(Field field) {
        return field.safeAccept(new FieldVisitor() {
            String fieldType;

            public void visitInteger(IntegerField field) throws Exception {
                fieldType = "IntegerField";
            }

            public void visitIntegerArray(IntegerArrayField field) throws Exception {
                fieldType = "IntegerArrayField";
            }

            public void visitDouble(DoubleField field) throws Exception {
                fieldType = "DoubleField";
            }

            public void visitDoubleArray(DoubleArrayField field) throws Exception {
                fieldType = "DoubleArrayField";
            }

            public void visitBigDecimal(BigDecimalField field) throws Exception {
                fieldType = "BigDecimalField";
            }

            public void visitBigDecimalArray(BigDecimalArrayField field) throws Exception {
                fieldType = "BigDecimalArrayField";
            }

            public void visitString(StringField field) throws Exception {
                fieldType = "StringField";
            }

            public void visitStringArray(StringArrayField field) throws Exception {
                fieldType = "StringArrayField";
            }

            public void visitBoolean(BooleanField field) throws Exception {
                fieldType = "BooleanField";
            }

            public void visitBooleanArray(BooleanArrayField field) throws Exception {
                fieldType = "BooleanArrayField";
            }

            public void visitLong(LongField field) throws Exception {
                fieldType = "LongField";
            }

            public void visitLongArray(LongArrayField field) throws Exception {
                fieldType = "LongArrayField";
            }

            public void visitDate(DateField field) throws Exception {
                fieldType = "DateField";
            }

            public void visitDateTime(DateTimeField field) throws Exception {
                fieldType = "DateTimeField";
            }

            public void visitBlob(BlobField field) throws Exception {
                fieldType = "BlobField";
            }

            public void visitGlob(GlobField field) throws Exception {
                fieldType = "GlobField";
            }

            public void visitGlobArray(GlobArrayField field) throws Exception {
                fieldType = "GlobArrayField";
            }

            public void visitUnionGlob(GlobUnionField field) throws Exception {
                fieldType = "GlobUnionField";
            }

            public void visitUnionGlobArray(GlobArrayUnionField field) throws Exception {
                fieldType = "GlobArrayUnionField";
            }
        }).fieldType;
    }
}
