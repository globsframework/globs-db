package org.globsframework.sql.utils;

import org.globsframework.sql.drivers.jdbc.DbServicesTestCase;
import org.globsframework.sql.model.DummyObject;
import org.junit.Test;

import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import java.io.File;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import static org.junit.Assert.*;

/**
 * The generator emitted the pre-migration shape: the old org.globsframework.metamodel packages,
 * GlobTypeLoaderFactory, GlobCreateFromAnnotation, FieldNameAnnotation — none of which exist any
 * more. What it produced could not be compiled, which is what this checks now.
 */
public class GenerateTypeFromDbTest extends DbServicesTestCase {

    @Test
    public void theGeneratedSourceCompiles() throws Exception {
        String source = generate();

        Path directory = Files.createTempDirectory("globs-generated");
        Path file = directory.resolve(className(source) + ".java");
        Files.writeString(file, source, StandardCharsets.UTF_8);

        JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull("a JDK is needed to run this test", compiler);
        int result = compiler.run(null, null, null,
                "-classpath", System.getProperty("java.class.path"),
                "-d", directory.toString(),
                file.toString());

        assertEquals(source, 0, result);
        // the generated source declares package org.globsframework.generated
        assertTrue(new File(directory.toFile(),
                "org/globsframework/generated/" + className(source) + ".class").exists());
    }

    @Test
    public void itDeclaresTheTypeThroughTheBuilder() throws Exception {
        String source = generate();

        assertTrue(source, source.contains("GlobTypeBuilderFactory.create("));
        assertTrue(source, source.contains("TYPE = builder.build();"));
        assertTrue(source, source.contains("import org.globsframework.core.metamodel.GlobType;"));
        // all of these went away with the Java annotation style
        assertFalse(source, source.contains("GlobTypeLoaderFactory"));
        assertFalse(source, source.contains("GlobCreateFromAnnotation"));
        assertFalse(source, source.contains("FieldNameAnnotation"));
        assertFalse(source, source.contains("org.globsframework.metamodel."));
    }

    @Test
    public void aNameThatIsNotAJavaIdentifierIsMadeIntoOne() {
        assertEquals("order_id", GenerateTypeFromDb.identifier("order-id"));
        assertEquals("_2nd", GenerateTypeFromDb.identifier("2nd"));
        assertEquals("plain", GenerateTypeFromDb.identifier("plain"));
        assertEquals("Order_id", GenerateTypeFromDb.className("order-id"));
    }

    private String generate() throws Exception {
        StringWriter writer = new StringWriter();
        new GenerateTypeFromDb(null, null, null, null, List.of())
                .extractType(writer, sqlConnection, sqlService.getTableName(DummyObject.TYPE, false));
        return writer.toString();
    }

    private String className(String source) {
        int start = source.indexOf("public class ") + "public class ".length();
        return source.substring(start, source.indexOf(' ', start));
    }
}
