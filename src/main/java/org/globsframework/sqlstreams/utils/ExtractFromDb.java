package org.globsframework.sqlstreams.utils;

import com.google.gson.Gson;
import org.globsframework.json.GlobsGson;
import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.fields.*;
import org.globsframework.model.Glob;
import org.globsframework.saxstack.utils.XmlUtils;
import org.globsframework.sqlstreams.SelectQuery;
import org.globsframework.sqlstreams.constraints.Constraint;
import org.globsframework.sqlstreams.constraints.Constraints;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcConnection;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcSqlService;
import org.globsframework.utils.Strings;

import java.io.*;
import java.time.LocalDate;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.lang.System.out;

public class ExtractFromDb {

    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final String tableName;
    private final List<String> ids;
    private final StringifyFieldVisitor visitor = new StringifyFieldVisitor();

    public ExtractFromDb(String jdbcUrl, String user, String password, String tableName, List<String> ids) {
        this.jdbcUrl = jdbcUrl;
        this.user = user;
        this.password = password;
        this.tableName = tableName;
        this.ids = ids;
    }

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            out.println("argument expected : [-type|-data|path (if path dump classname.json and classname.xml] jdbcURL user password tableName id1 id2 id3 etc");
            System.exit(1);
        }

        int count = 0;
        List<String> ids = new ArrayList<>();
        for (String arg : args) {
            if (++count > 5) {
                ids.add(arg);
            }
        }

        ExtractFromDb extractFromDb = new ExtractFromDb(args[1], args[2], args[3], args[4], ids);
        Writer writer = new BufferedWriter(new OutputStreamWriter(System.out));
        if (args[0].startsWith("-t")) {
            extractFromDb.extractType(writer);
        } else if (args[0].startsWith("-d")) {
            extractFromDb.extractData(writer);
        } else {
            extractFromDb.extractAll(args[0]);
        }
        writer.flush();
    }

    private void extractAll(String path) throws IOException {
        FileOutputStream jsonFile =
                new FileOutputStream(new File(path, tableName + ".json"));

        FileOutputStream xmlFile =
                new FileOutputStream(new File(path, tableName + ".xml"));

        extractType(new BufferedWriter(new OutputStreamWriter(jsonFile, "UTF-8")));
        extractData(new BufferedWriter(new OutputStreamWriter(xmlFile, "UTF-8")));
    }

    private void extractType(Writer writer) throws IOException {
        JdbcSqlService jdbcSqlService = new JdbcSqlService(jdbcUrl, user, password);
        GlobType globType = jdbcSqlService.getDb().extractType(tableName, Collections.emptySet());
        Gson gson = GlobsGson.create(name -> globType);
        String json = gson.toJson(globType);
        writer.append(json);
    }

    private void extractData(Writer writer) throws IOException {
        JdbcSqlService jdbcSqlService = new JdbcSqlService(jdbcUrl, user, password);
        JdbcConnection db = jdbcSqlService.getDb();
        GlobType globType = db.extractType(tableName, Collections.emptySet());
        if (globType == null) {
            throw new RuntimeException("Fail to extract " + tableName + " from " + jdbcUrl);
        }
        Constraint constraint = null;
        if (!ids.isEmpty()) {
            Field[] keyFields = globType.getKeyFields();
            if (keyFields.length != 1) {
                throw new RuntimeException("Only on  keyfield is expected bug got " + Arrays.toString(keyFields));
            }
            Field keyField = keyFields[0];
            Set filterIn;
            if (keyField instanceof IntegerField) {
                filterIn = ids.stream().map(Integer::parseInt).collect(Collectors.toSet());
            } else {
                filterIn = new HashSet(ids);
            }
            constraint = Constraints.in(keyField, filterIn);
        }

        SelectQuery query = db.getQueryBuilder(globType, constraint).selectAll()
                .getQuery();

        Stream<Glob> globStream = query.executeAsGlobStream();
        writer.append("<globs>\n");
        globStream.forEach(glob -> {
            try {
                writer.append(writeXml(globType, glob));
                writer.append("\n");
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        writer.append("</globs>");
        query.close();
    }

    private String writeXml(GlobType type, Glob glob) {
        StringBuilder stringBuilder = new StringBuilder();
        stringBuilder.append("<").append(type.getName()).append(" ");
        Field[] fields = type.getFields();
        for (Field field : fields) {
            if (!glob.isNull(field)) {
                Object val = field.safeVisit(visitor, glob).val;
                stringBuilder.append(" ");
                stringBuilder.append(field.getName());
                stringBuilder.append("=\"");
                stringBuilder.append(XmlUtils.convertEntities(Strings.toString(val)));
                stringBuilder.append("\"");
            }
        }
        stringBuilder.append("/>");
        return stringBuilder.toString();
    }

    private static class StringifyFieldVisitor extends FieldVisitorWithContext.AbstractFieldVisitor<Glob> {
        private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyy/MM/dd");
        private static final DateTimeFormatter TIMESTAMP_FORMAT = DateTimeFormatter.ofPattern("yyyy/MM/dd HH:mm:ss");
        Object val;

        public void visitBoolean(BooleanField field, Glob context) throws Exception {
            Boolean aBoolean = context.get(field);
            if (aBoolean != null) {
                val = aBoolean ? "true" : "false";
            }
        }

        public void visitDate(DateField field, Glob context) throws Exception {
            LocalDate localDate = context.get(field);
            val = DATE_FORMAT.format(localDate);
        }

        public void visitDateTime(DateTimeField field, Glob context) throws Exception {
            ZonedDateTime zonedDateTime = context.get(field);
            val = TIMESTAMP_FORMAT.format(zonedDateTime);
        }

        public void notManaged(Field field, Glob context) throws Exception {
            val = context.getValue(field);
        }
    }
}
