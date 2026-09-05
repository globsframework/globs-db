package org.globsframework.sql.drivers.jdbc.pool;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Optional;
import java.util.Properties;

/**
 * Entry point to the optional connection pool.
 * <p>
 * HikariCP is an <em>optional</em> dependency of this artifact: it is not transitive, so an
 * application that does not want it simply does not put it on its classpath. Everything that
 * touches HikariCP classes lives in {@link HikariConnectionPool}, which is only reached once
 * {@link #isAvailable()} has confirmed the library is there — and even then the call is guarded
 * against a LinkageError, so a partial or shaded classpath degrades to the unpooled path instead of
 * failing at startup.
 */
public class ConnectionPools {
    private static final Logger LOGGER = LoggerFactory.getLogger(ConnectionPools.class);
    private static final String HIKARI_CLASS = "com.zaxxer.hikari.HikariDataSource";

    private static Boolean available;

    private ConnectionPools() {
    }

    /**
     * True when a pool implementation is on the classpath.
     */
    public static synchronized boolean isAvailable() {
        if (available == null) {
            try {
                Class.forName(HIKARI_CLASS, false, ConnectionPools.class.getClassLoader());
                available = Boolean.TRUE;
            } catch (ClassNotFoundException e) {
                available = Boolean.FALSE;
            }
        }
        return available;
    }

    /**
     * Builds a pooled DataSource, or an empty Optional when pooling is disabled by the config or no
     * pool implementation is available. The caller owns the returned DataSource and must hand it
     * back to {@link #close(DataSource)}.
     *
     * @param driverClassName may be null, letting DriverManager resolve the driver from the url
     */
    public static Optional<DataSource> create(String url, Properties dbInfo, PoolConfig config,
                                              String driverClassName) {
        if (config == null || !config.isPooling()) {
            return Optional.empty();
        }
        if (!isAvailable()) {
            LOGGER.info("{} not found on the classpath: one physical connection per getDb(). " +
                        "Add com.zaxxer:HikariCP to enable pooling.", HIKARI_CLASS);
            return Optional.empty();
        }
        try {
            DataSource dataSource = HikariConnectionPool.create(url, dbInfo, config, driverClassName);
            LOGGER.info("Pooled connections for {} : {}", url, config);
            return Optional.of(dataSource);
        } catch (LinkageError e) {
            LOGGER.warn("Pool implementation present but unusable, falling back to direct connections", e);
            return Optional.empty();
        }
    }

    /**
     * Closes a DataSource this library created. Anything else is left alone.
     */
    public static void close(DataSource dataSource) {
        if (dataSource instanceof AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception e) {
                LOGGER.warn("Failed to close the connection pool", e);
            }
        }
    }
}
