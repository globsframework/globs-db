package org.globsframework.sql.testdb;

import org.globsframework.sql.drivers.jdbc.JdbcSqlService;
import org.globsframework.sql.drivers.jdbc.pool.PoolConfig;
import org.junit.Assume;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;

/**
 * The backend the suite runs against.
 * <p>
 * HSQLDB in memory is the default, so {@code mvn test} still needs nothing installed. Pass
 * {@code -Dglobs.test.db=postgresql} and the same tests run against a PostgreSQL started in a
 * container — which is where the dialect bugs are, identifier escaping first among them. The
 * container is started once for the whole JVM and shared by every test class.
 * <p>
 * When no container runtime is reachable, the tests that need one are skipped rather than failed.
 */
public class TestDb {
    public static final String BACKEND_PROPERTY = "globs.test.db";
    public static final String IMAGE_PROPERTY = "globs.test.db.image";

    private static final String HSQLDB_URL = "jdbc:hsqldb:.";
    private static final String DEFAULT_POSTGRES_IMAGE = "postgres:16";

    private static PostgreSQLContainer<?> postgres;

    public enum Backend {
        hsqldb,
        postgresql
    }

    private TestDb() {
    }

    public static Backend current() {
        String value = System.getProperty(BACKEND_PROPERTY, Backend.hsqldb.name());
        try {
            return Backend.valueOf(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown -D" + BACKEND_PROPERTY + "=" + value
                                               + ", expected one of " + Arrays.toString(Backend.values()));
        }
    }

    public static JdbcSqlService createService() {
        return createService(PoolConfig.DEFAULT);
    }

    public static JdbcSqlService createService(PoolConfig poolConfig) {
        return switch (current()) {
            case hsqldb -> new JdbcSqlService(HSQLDB_URL, "sa", "", poolConfig);
            case postgresql -> {
                PostgreSQLContainer<?> container = postgres();
                yield new JdbcSqlService(container.getJdbcUrl(), container.getUsername(),
                        container.getPassword(), poolConfig);
            }
        };
    }

    /**
     * Skips the calling test unless the suite runs against this backend. For the tests that assert
     * on one dialect only.
     */
    public static void assumeBackend(Backend backend) {
        Assume.assumeTrue("only runs on " + backend + ", use -D" + BACKEND_PROPERTY + "=" + backend,
                current() == backend);
    }

    private static synchronized PostgreSQLContainer<?> postgres() {
        if (postgres == null) {
            Assume.assumeTrue("no container runtime reachable, skipping the " + Backend.postgresql + " run",
                    DockerClientFactory.instance().isDockerAvailable());
            String image = System.getProperty(IMAGE_PROPERTY, DEFAULT_POSTGRES_IMAGE);
            PostgreSQLContainer<?> container = new PostgreSQLContainer<>(
                    DockerImageName.parse(image).asCompatibleSubstituteFor("postgres"))
                    .withDatabaseName("globs")
                    .withUsername("globs")
                    .withPassword("globs");
            container.start();
            // Testcontainers cleans up through Ryuk, which is disabled on some local setups
            Runtime.getRuntime().addShutdownHook(new Thread(container::stop));
            postgres = container;
        }
        return postgres;
    }
}
