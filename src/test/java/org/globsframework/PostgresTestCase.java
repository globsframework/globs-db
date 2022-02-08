package org.globsframework;

import com.google.gson.Gson;
import org.globsframework.json.GlobsGson;
import org.globsframework.json.annottations.IsJsonContentType;
import org.globsframework.metamodel.GlobModel;
import org.globsframework.metamodel.GlobModelBuilder;
import org.globsframework.metamodel.GlobType;
import org.globsframework.metamodel.GlobTypeBuilder;
import org.globsframework.metamodel.annotations.AllAnnotations;
import org.globsframework.metamodel.annotations.IsDateTime;
import org.globsframework.metamodel.fields.DoubleField;
import org.globsframework.metamodel.fields.LongField;
import org.globsframework.metamodel.fields.StringField;
import org.globsframework.metamodel.impl.DefaultGlobTypeBuilder;
import org.globsframework.metamodel.type.DataType;
import org.globsframework.model.Glob;
import org.globsframework.model.GlobList;
import org.globsframework.model.MutableGlob;
import org.globsframework.sqlstreams.GlobTypeExtractor;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.SqlService;
import org.globsframework.sqlstreams.annotations.DbFieldIsNullable;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcSqlService;
import org.globsframework.utils.serialization.CompressedSerializationOutput;
import org.globsframework.utils.serialization.SerializedOutput;
import org.junit.Assert;
import org.junit.Test;

import java.io.*;
import java.time.ZonedDateTime;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

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
            GlobModel globTypes = GlobModelBuilder.create(AllAnnotations.MODEL).add(IsJsonContentType.TYPE)
                    .add(org.globsframework.sqlstreams.annotations.AllAnnotations.MODEL)
                    .get();

            Gson gson = GlobsGson.create(globTypes::getType);

            String typeAsJson = gson.toJson(type);

            System.out.printf(typeAsJson);
            FileWriter fileWriter = new FileWriter(outputFile + ".meta");
            fileWriter.write(typeAsJson);
            fileWriter.close();

            fileInputStream = new BufferedOutputStream(new FileOutputStream(outputFile + ".ser"));

            SerializedOutput serializedOutput = new CompressedSerializationOutput(fileInputStream);


            long count = db.getQueryBuilder(type)
                    .selectAll().getQuery().executeAsGlobStream()
                    .peek(glob -> serializedOutput.writeGlob(glob))
                    .count();

            fileInputStream.close();
            System.out.println("TheozReadFromDb.read " + count + " lines read");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            db.commitAndClose();
        }
    }

    @Test
    public void name() throws IOException {
        SqlService sqlService = new JdbcSqlService("jdbc:postgresql://localhost/glindaBackend", "glindaBackend", "glinda");
        SqlConnection db = sqlService.getDb();
        GlobTypeBuilder globTypeBuilder = DefaultGlobTypeBuilder.init("TEST");
        StringField f1 = globTypeBuilder.declareStringField("f1");
        DoubleField f2 = globTypeBuilder.declareDoubleField("f2");
        Glob nullable = DbFieldIsNullable.create(true);
        Glob idDateTime = IsDateTime.TYPE.instantiate();
        LongField f3 = globTypeBuilder.declareLongField("f3", idDateTime);

        GlobType globType = globTypeBuilder.get();

        MutableGlob data = globType.instantiate()
                .set(f1, "ééé")
                .set(f2, 3.3)
                .set(f3, ZonedDateTime.now().toInstant().toEpochMilli())
                ;

        db.createTable(globType);
        db.populate(new GlobList(data));

        db.commitAndClose();

//        Path theOz = Files.createTempFile("theOz", "");
//        System.out.println("TheozReadFromDb.name " + theOz.toAbsolutePath().toString());
//        main("jdbc:postgresql://localhost/glindaBackend", "glindaBackend", "glinda", globType.getName(), theOz.toAbsolutePath().toString());
//        Path theOz1 = Paths.get(theOz.toUri(), ".ser");
//        Assert.assertTrue(theOz1.toFile().exists());
//        Assert.assertTrue(theOz.toFile().length() > 10);

        Assert.assertTrue(true);

    }
}
