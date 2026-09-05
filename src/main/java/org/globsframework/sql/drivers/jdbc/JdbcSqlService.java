package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.utils.exceptions.ItemNotFound;
import org.globsframework.core.utils.exceptions.UnexpectedApplicationState;
import org.globsframework.sql.drivers.hsqldb.HsqlConnection;
import org.globsframework.sql.drivers.jdbc.pool.ConnectionPools;
import org.globsframework.sql.drivers.jdbc.pool.PoolConfig;
import org.globsframework.sql.drivers.mysql.MysqlConnection;
import org.globsframework.sql.drivers.oracle.OracleConnection;
import org.globsframework.sql.drivers.postgresql.PostgresqlConnection;
import org.globsframework.sql.utils.AbstractSqlService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * SqlService built from a JDBC url. The driver is loaded reflectively from the url prefix, so this
 * artifact depends on no driver: the application ships the one it needs.
 * <p>
 * Connections are pooled when a pool implementation is on the classpath (see
 * {@link ConnectionPools}); otherwise every getDb() opens a physical connection, which is what this
 * class always used to do. Pass {@link PoolConfig#NO_POOL} to force that older behaviour.
 * <p>
 * The service owns its pool, so it must be {@link #close() closed} when the application shuts down.
 */
public class JdbcSqlService extends AbstractSqlService {
    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcSqlService.class);
    private static final Map<String, Driver> loadedDrivers = new ConcurrentHashMap<>();

    private final String dbName;
    private final Properties dbInfo;
    private final DbType dbType;
    private Driver driver;
    private DataSource dataSource;
    private volatile boolean closed;

    public JdbcSqlService(String dbName, String user, String password) {
        this(dbName, user, password, DefaultNamingMapping.INSTANCE, PoolConfig.DEFAULT);
    }

    public JdbcSqlService(String dbName, String user, String password, NamingMapping namingMapping) {
        this(dbName, user, password, namingMapping, PoolConfig.DEFAULT);
    }

    public JdbcSqlService(String dbName, String user, String password, PoolConfig poolConfig) {
        this(dbName, user, password, DefaultNamingMapping.INSTANCE, poolConfig);
    }

    public JdbcSqlService(String dbName, String user, String password, NamingMapping namingMapping,
                          PoolConfig poolConfig) {
        super(getMapping(dbName, namingMapping));
        this.dbName = dbName;
        this.dbType = DbType.fromString(dbName);
        this.dbInfo = new Properties();
        if (user != null) {
            dbInfo.put("user", user);
        }
        if (password != null) {
            dbInfo.put("password", password);
        }
        if (dbType == DbType.mysql || dbType == DbType.mariadb) {
            dbInfo.put("zeroDateTimeBehavior", "convertToNull");
        }
        init(poolConfig);
    }

    static NamingMapping getMapping(String dbName, NamingMapping namingMapping) {
        if (dbName.contains("hsqldb")) {
            return new HsqlDbNamingMapping();
        } else if (dbName.contains("mysql") || dbName.startsWith("jdbc:mariadb:")) {
            return new DefaultNamingMapping();
        } else if (dbName.startsWith("jdbc:postgresql:")) {
            return new DefaultNamingMapping();
        } else if (dbName.startsWith("jdbc:oracle:")) {
            return new DefaultNamingMapping();
        } else {
            return namingMapping;
        }
    }

    private void init(PoolConfig poolConfig) {
        try {
            registerDriver();
            // driverClassName is left to DriverManager: registerDriver() has already made the
            // driver known to it, and the SPI covers the drivers we do not name.
            dataSource = ConnectionPools.create(dbName, dbInfo, poolConfig, null).orElse(null);
            if (dataSource == null && dbType == DbType.hsqldb) {
                dataSource = createHsqldbPool();
            }
        } catch (Exception e) {
            throw new ItemNotFound(e);
        }
    }

    private void registerDriver() throws Exception {
        String driverClassName = switch (dbType) {
            case hsqldb -> "org.hsqldb.jdbc.JDBCDriver";
            case mysql -> "com.mysql.jdbc.Driver";
            case mariadb -> "org.mariadb.jdbc.Driver";
            case postgresql -> "org.postgresql.Driver";
            case oracle -> "oracle.jdbc.OracleDriver";
        };
        driver = loadedDrivers.get(dbName);
        if (driver == null) {
            driver = (Driver) Class.forName(driverClassName).getDeclaredConstructor().newInstance();
            loadedDrivers.put(dbName, driver);
        }
    }

    /**
     * HSQLDB ships its own pool, used when no general purpose one is available.
     */
    private DataSource createHsqldbPool() throws Exception {
        DataSource hsqldbPool = (DataSource) Class.forName("org.hsqldb.jdbc.JDBCPool")
                .getDeclaredConstructor().newInstance();
        hsqldbPool.getClass().getDeclaredMethod("setUrl", String.class).invoke(hsqldbPool, dbName);
        hsqldbPool.getClass().getDeclaredMethod("setProperties", Properties.class).invoke(hsqldbPool, dbInfo);
        return hsqldbPool;
    }

    public JdbcConnection getDb() {
        return create(false);
    }

    public JdbcConnection getAutoCommitDb() {
        return create(true);
    }

    private JdbcConnection create(boolean autoCommit) {
        Connection connection = getConnection();
        try {
            connection.setAutoCommit(autoCommit);
        } catch (SQLException e) {
            closeQuietly(connection);
            throw new UnexpectedApplicationState(e);
        }
        return switch (dbType) {
            case postgresql -> new PostgresqlConnection(autoCommit, connection, this);
            case hsqldb -> new HsqlConnection(autoCommit, connection, this);
            case mysql, mariadb -> new MysqlConnection(autoCommit, connection, this);
            case oracle -> new OracleConnection(autoCommit, connection, this);
        };
    }

    /**
     * A connection from the pool, or a new physical one when there is no pool. The caller owns it.
     */
    public Connection getConnection() {
        if (closed) {
            throw new UnexpectedApplicationState("SqlService is closed for " + dbName);
        }
        try {
            if (dataSource != null) {
                return dataSource.getConnection();
            }
            return driver.connect(dbName, dbInfo);
        } catch (SQLException e) {
            throw new UnexpectedApplicationState("for " + dbInfo.get("user") + " on " + dbName, e);
        }
    }

    /**
     * True when connections come from a pool rather than being opened one per getDb().
     */
    public boolean isPooled() {
        return dataSource != null;
    }

    /**
     * Closes the pool this service owns. Connections already handed out are not closed here — the
     * code holding them is responsible for that.
     */
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        if (dataSource != null) {
            ConnectionPools.close(dataSource);
            dataSource = null;
        }
    }

    private static void closeQuietly(Connection connection) {
        try {
            connection.close();
        } catch (SQLException e) {
            LOGGER.warn("Failed to release connection", e);
        }
    }
}
