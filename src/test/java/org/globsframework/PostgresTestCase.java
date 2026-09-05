package org.globsframework;

import com.google.gson.Gson;
import org.globsframework.core.metamodel.GlobModel;
import org.globsframework.core.metamodel.GlobModelBuilder;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.annotations.AllCoreAnnotations;
import org.globsframework.core.metamodel.annotations.IsDateTime;
import org.globsframework.core.metamodel.fields.DoubleField;
import org.globsframework.core.metamodel.fields.LongField;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.metamodel.impl.DefaultGlobTypeBuilder;
import org.globsframework.core.metamodel.type.DataType;
import org.globsframework.core.model.Glob;
import org.globsframework.core.model.MutableGlob;
import org.globsframework.core.utils.serialization.CompressedSerializationOutput;
import org.globsframework.core.utils.serialization.GlobSerializer;
import org.globsframework.core.utils.serialization.SerializedInputOutputFactory;
import org.globsframework.core.utils.serialization.SerializedOutput;
import org.globsframework.json.GlobsGson;
import org.globsframework.json.annottations.IsJsonContent;
import org.globsframework.sql.GlobTypeExtractor;
import org.globsframework.sql.SelectQuery;
import org.globsframework.sql.SqlConnection;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.annotations.AllSqlAnnotations;
import org.globsframework.sql.drivers.jdbc.JdbcSqlService;
import org.globsframework.sql.testdb.TestDb;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.time.ZonedDateTime;
import java.util.*;
import java.util.stream.Stream;

public class PostgresTestCase {

    public static void main(String... args) throws IOException {
        new PostgresTestCase()
                .read(args[0], args[1], args[2], args[3],
                        args.length <= 4 ? Collections.emptySet() :
                                new HashSet<String>(Arrays.asList(Arrays.copyOfRange(args, 4, args.length))),
                        args[4]);
    }

    public void read(String jdbcUrl, String user, String pwd, String tableName,
                     Collection<String> fieldToExclude, String outputFile) throws IOException {
        SqlService sqlService = new JdbcSqlService(jdbcUrl, user, pwd);
        OutputStream fileInputStream;
        SqlConnection db = sqlService.getDb();
        try {
            GlobType type = db.extractType(tableName)
                    .forceType(new GlobTypeExtractor.Transtype() {
                        public DataType getType(String name, DataType sqlType) {
                            return sqlType == DataType.Date ? DataType.Integer : sqlType == DataType.DateTime ? DataType.Long : sqlType;
                        }
                    })
                    .extract();

            if (type == null) {
                throw new RuntimeException(tableName + " not found");
            }
            GlobModel globTypes = GlobModelBuilder.create(AllCoreAnnotations.MODEL).add(IsJsonContent.TYPE)
                    .add(AllSqlAnnotations.MODEL)
                    .get();

            Gson gson = GlobsGson.create(globTypes::getType);

            String typeAsJson = gson.toJson(type);

            System.out.printf(typeAsJson);
            FileWriter fileWriter = new FileWriter(outputFile + ".meta");
            fileWriter.write(typeAsJson);
            fileWriter.close();

            fileInputStream = new BufferedOutputStream(new FileOutputStream(outputFile + ".ser"));

            SerializedOutput serializedOutput = SerializedInputOutputFactory.init(fileInputStream);
            GlobSerializer globSerializer = new GlobSerializer(serializedOutput);

            long count;
            try (SelectQuery query = db.getQueryBuilder(type)
                    .selectAll().getQuery()) {
                try (Stream<Glob> globStream = query.executeAsGlobStream()) {
                    count = globStream
                            .peek(globSerializer::writeGlob)
                            .count();
                }
            }

            fileInputStream.close();
            System.out.println("TheozReadFromDb.read " + count + " lines read");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.commitAndClose();
        }
    }

    /**
     * Round-trips a type declared on the fly — accented text, a double and a datetime — through
     * createTable/populate on whichever backend the suite runs against. Used to be @Ignore'd against
     * a hardcoded personal database.
     */
    @Test
    public void createTableAndPopulate() throws IOException {
        GlobTypeBuilder globTypeBuilder = DefaultGlobTypeBuilder.init("TEST_POPULATE");
        StringField f1 = globTypeBuilder.declareStringField("f1");
        DoubleField f2 = globTypeBuilder.declareDoubleField("f2");
        LongField f3 = globTypeBuilder.declareLongField("f3", IsDateTime.TYPE.instantiate());
        GlobType globType = globTypeBuilder.build();

        long now = ZonedDateTime.now().toInstant().toEpochMilli();
        MutableGlob data = globType.instantiate()
                .set(f1, "ééé")
                .set(f2, 3.3)
                .set(f3, now);

        try (JdbcSqlService sqlService = TestDb.createService()) {
            sqlService.runInTransaction(db -> {
                db.createTable(globType);
                db.getDeleteRequest(globType).apply();
                db.populate(List.of(data));
            });

            Glob reloaded = sqlService.read(db -> db.getQueryBuilder(globType)
                    .selectAll()
                    .getQuery()
                    .executeUnique());

            Assert.assertEquals("ééé", reloaded.get(f1));
            Assert.assertEquals(3.3, reloaded.get(f2), 0.0001);
            Assert.assertEquals(Long.valueOf(now), reloaded.get(f3));
        }
    }
}
