package org.globsframework.sqlstreams.utils;

import com.google.gson.Gson;
import org.globsframework.json.GlobsGson;
import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.drivers.jdbc.JdbcSqlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.System.out;

public class ExtractTypeFromDb {
    private static Logger LOGGER = LoggerFactory.getLogger(ExtractTypeFromDb.class);
    private final String path;
    private final String jdbcUrl;
    private final String user;
    private final String password;
    private final List<String> tableName;

    public ExtractTypeFromDb(String path, String jdbcUrl, String user, String password, List<String> tableName) {
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

        ExtractTypeFromDb extractFromDb = new ExtractTypeFromDb(args[0], args[1], args[2], args[3], tableName);
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
        Gson gson = GlobsGson.create(name -> globType);
        String json = gson.toJson(globType);
        writer.append(json);
        writer.flush();
    }
}
