package org.globsframework;

import com.google.gson.Gson;
import org.globsframework.core.metamodel.GlobModel;
import org.globsframework.core.metamodel.GlobModelBuilder;
import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.GlobTypeBuilder;
import org.globsframework.core.metamodel.annotations.AllAnnotations;
import org.globsframework.core.metamodel.fields.DoubleField;
import org.globsframework.core.metamodel.fields.StringField;
import org.globsframework.core.metamodel.impl.DefaultGlobTypeBuilder;
import org.globsframework.core.metamodel.type.DataType;
import org.globsframework.core.model.MutableGlob;
import org.globsframework.core.utils.serialization.CompressedSerializationOutput;
import org.globsframework.core.utils.serialization.SerializedOutput;
import org.globsframework.json.GlobsGson;
import org.globsframework.json.annottations.IsJsonContent;
import org.globsframework.sql.GlobTypeExtractor;
import org.globsframework.sql.SqlConnection;
import org.globsframework.sql.SqlService;
import org.globsframework.sql.drivers.jdbc.JdbcSqlService;
import org.junit.Ignore;
import org.junit.Test;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class TheozReadFromDb {

    public static void main(String... args) throws IOException {
        new TheozReadFromDb()
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
            GlobModel globTypes = GlobModelBuilder.create(AllAnnotations.MODEL).add(IsJsonContent.TYPE)
                    .add(org.globsframework.sql.annotations.AllAnnotations.MODEL)
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
    @Ignore
    public void name() throws IOException {
        SqlService sqlService = new JdbcSqlService("jdbc:hsqldb:.", "sa", "");
        SqlConnection db = sqlService.getDb();
        GlobTypeBuilder globTypeBuilder = DefaultGlobTypeBuilder.init("TEST");
        StringField f1 = globTypeBuilder.declareStringField("f1");
        DoubleField f2 = globTypeBuilder.declareDoubleField("f2");

        GlobType globType = globTypeBuilder.get();

        MutableGlob data = globType.instantiate().set(f1, "ééé")
                .set(f2, 3.3);

        db.createTable(globType);
        db.populate(List.of(data));

        Path theOz = Files.createTempFile("theOz", "");
        System.out.println("TheozReadFromDb.name " + theOz.toAbsolutePath().toString());
        main("jdbc:hsqldb:.", "sa", "", globType.getName(), theOz.toAbsolutePath().toString());
//        Path theOz1 = Paths.get(theOz.toUri(), ".ser");
//        Assert.assertTrue(theOz1.toFile().exists());
//        Assert.assertTrue(theOz.toFile().length() > 10);
    }
}
