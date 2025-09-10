package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.utils.exceptions.ItemNotFound;
import org.globsframework.core.utils.exceptions.UnexpectedApplicationState;
import org.globsframework.sql.drivers.hsqldb.HsqlConnection;
import org.globsframework.sql.drivers.mysql.MysqlConnection;
import org.globsframework.sql.drivers.oracle.OracleConnection;
import org.globsframework.sql.drivers.postgresql.PostgresqlConnection;
import org.globsframework.sql.utils.AbstractSqlService;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class JdbcSqlService extends AbstractSqlService {
    private static Map<String, Driver> loadedDrivers = new ConcurrentHashMap<>();
    private Driver driver;
    private String dbName;
    private Properties dbInfo;
    private DbFactory dbFactory;

    public JdbcSqlService(String dbName, String user, String password, NamingMapping namingMapping) {
        super(getMapping(dbName, namingMapping));
        this.dbName = dbName;
        dbInfo = new Properties();
        dbInfo.put("user", user);
        dbInfo.put("password", password);
        loadDriver();
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

    public JdbcSqlService(String dbName, String user, String password) {
        this(dbName, user, password, DefaultNamingMapping.INSTANCE);
    }

    interface DbFactory {
        JdbcConnection create(boolean autoCommit);
    }

    private void loadDriver() {
        try {
            if (dbName.contains("hsqldb")) {
                setupHsqldb();
            } else if (dbName.contains("mysql")) {
                setupMySql();
            } else if (dbName.startsWith("jdbc:mariadb:")) {
                setupMariaDb();
            } else if (dbName.startsWith("jdbc:postgresql:")) {
                setupPostgresql();
            } else if (dbName.startsWith("jdbc:oracle:")) {
                setupOrableSql();
            }
        } catch (Exception e) {
            throw new ItemNotFound(e);
        }
    }

    private void setupPostgresql() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
        driver = loadedDrivers.get(dbName);
        if (driver == null) {
            driver = (Driver) Class.forName("org.postgresql.Driver").getDeclaredConstructor().newInstance();
            loadedDrivers.put(dbName, driver);
        }

        dbFactory = autoCommit -> {
            Connection connection = getConnection();
            try {
                connection.setAutoCommit(autoCommit);
            } catch (SQLException e) {
                throw new UnexpectedApplicationState(e);
            }
            return new PostgresqlConnection(autoCommit, connection, JdbcSqlService.this);
        };
    }

    private void setupOrableSql() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
        driver = loadedDrivers.get(dbName);
        if (driver == null) {
            driver = (Driver) Class.forName("oracle.jdbc.OracleDriver").getDeclaredConstructor().newInstance();
            loadedDrivers.put(dbName, driver);
        }

        dbFactory = autoCommit -> {
            Connection connection = getConnection();
            try {
                connection.setAutoCommit(autoCommit);
            } catch (SQLException e) {
                throw new UnexpectedApplicationState(e);
            }
            return new OracleConnection(autoCommit, connection, JdbcSqlService.this);
        };
    }

    private void setupMariaDb() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
        driver = loadedDrivers.get(dbName);
        if (driver == null) {
            driver = (Driver) Class.forName("org.mariadb.jdbc.Driver").getDeclaredConstructor().newInstance();
            loadedDrivers.put(dbName, driver);
        }
        dbInfo.put("zeroDateTimeBehavior", "convertToNull");
        dbFactory = autoCommit -> {
            Connection connection = getConnection();
            try {
                connection.setAutoCommit(autoCommit);
            } catch (SQLException e) {
                throw new UnexpectedApplicationState(e);
            }

            return new MysqlConnection(autoCommit, connection, JdbcSqlService.this);
        };
    }

    private void setupMySql() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
        driver = loadedDrivers.get(dbName);
        if (driver == null) {
            driver = (Driver) Class.forName("com.mysql.jdbc.Driver").getDeclaredConstructor().newInstance();
            loadedDrivers.put(dbName, driver);
        }
        dbInfo.put("zeroDateTimeBehavior", "convertToNull");
        dbFactory = autoCommit -> {
            Connection connection = getConnection();
            try {
                connection.setAutoCommit(autoCommit);
            } catch (SQLException e) {
                throw new UnexpectedApplicationState(e);
            }

            return new MysqlConnection(autoCommit, connection, JdbcSqlService.this);
        };
    }

    private void setupHsqldb() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
        DataSource dataSource = (DataSource) Class.forName("org.hsqldb.jdbc.JDBCPool").getDeclaredConstructor().newInstance();
        dataSource.getClass().getDeclaredMethod("setUrl", String.class).invoke(dataSource, dbName);
        dataSource.getClass().getDeclaredMethod("setProperties", Properties.class).invoke(dataSource, dbInfo);
        dbFactory = autoCommit -> {
            try {
                Connection connection = dataSource.getConnection();
                connection.setAutoCommit(autoCommit);
                return new HsqlConnection(autoCommit, connection, JdbcSqlService.this);
            } catch (SQLException e) {
                throw new UnexpectedApplicationState(e);
            }
        };
    }

    public JdbcConnection getDb() {
        return dbFactory.create(false);
    }

    public JdbcConnection getAutoCommitDb() {
        return dbFactory.create(true);
    }

    synchronized public Connection getConnection() {
        try {
            return driver.connect(dbName, dbInfo);
        } catch (SQLException e) {
            throw new UnexpectedApplicationState("for " + dbInfo.get("user") + " on " + dbName, e);
        }
    }
}
