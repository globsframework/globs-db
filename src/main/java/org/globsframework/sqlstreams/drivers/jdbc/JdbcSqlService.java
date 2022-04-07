package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.annotations.DbFieldName;
import org.globsframework.sqlstreams.annotations.TargetTypeName;
import org.globsframework.sqlstreams.drivers.hsqldb.HsqlConnection;
import org.globsframework.sqlstreams.drivers.mysql.MysqlConnection;
import org.globsframework.sqlstreams.drivers.postgresql.PostgresqlConnection;
import org.globsframework.sqlstreams.utils.AbstractSqlService;
import org.globsframework.utils.exceptions.ItemNotFound;
import org.globsframework.utils.exceptions.UnexpectedApplicationState;

import java.lang.reflect.InvocationTargetException;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

public class JdbcSqlService extends AbstractSqlService {
    private static Map<String, Driver>  loadedDrivers = new ConcurrentHashMap<>();
    private Driver driver;
    private String dbName;
    private Properties dbInfo;
    private DbFactory dbFactory;
    private NamingMapping namingMapping;

    public JdbcSqlService(String dbName, String user, String password, NamingMapping namingMapping) {
        this.dbName = dbName;
        this.namingMapping = namingMapping;
        dbInfo = new Properties();
        dbInfo.put("user", user);
        dbInfo.put("password", password);
        loadDriver();
    }

    public JdbcSqlService(String dbName, String user, String password) {
        this(dbName, user, password, DefaultNamingMapping.INSTANCE);
    }

    public interface NamingMapping {
        String getTableName(GlobType globType);

        String getColumnName(Field field);
    }

    interface DbFactory {
        JdbcConnection create(boolean autoCommit);
    }

    public String getTableName(GlobType globType) {
        return namingMapping.getTableName(globType);
    }

    public String getColumnName(Field field) {
        return namingMapping.getColumnName(field);
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
        namingMapping = new ToLowerCaseNamingMapping(namingMapping);
        dbFactory = new DbFactory() {
            public JdbcConnection create(boolean autoCommit) {
                Connection connection = getConnection();
                try {
                    connection.setAutoCommit(autoCommit);
                } catch (SQLException e) {
                    throw new UnexpectedApplicationState(e);
                }
                return new PostgresqlConnection(autoCommit, connection, JdbcSqlService.this);
            }
        };
    }

    private void setupMariaDb() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
        driver = loadedDrivers.get(dbName);
        if (driver == null) {
            driver = (Driver) Class.forName("org.mariadb.jdbc.Driver").getDeclaredConstructor().newInstance();
            loadedDrivers.put(dbName, driver);
        }
        dbInfo.put("zeroDateTimeBehavior", "convertToNull");
        dbFactory = new DbFactory() {
            public JdbcConnection create(boolean autoCommit) {
                Connection connection = getConnection();
                try {
                    connection.setAutoCommit(autoCommit);
                } catch (SQLException e) {
                    throw new UnexpectedApplicationState(e);
                }

                return new MysqlConnection(autoCommit, connection, JdbcSqlService.this);
            }
        };
    }

    private void setupMySql() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
        driver = loadedDrivers.get(dbName);
        if (driver == null) {
            driver = (Driver) Class.forName("com.mysql.jdbc.Driver").getDeclaredConstructor().newInstance();
            loadedDrivers.put(dbName, driver);
        }
        dbInfo.put("zeroDateTimeBehavior", "convertToNull");
        dbFactory = new DbFactory() {
            public JdbcConnection create(boolean autoCommit) {
                Connection connection = getConnection();
                try {
                    connection.setAutoCommit(autoCommit);
                } catch (SQLException e) {
                    throw new UnexpectedApplicationState(e);
                }

                return new MysqlConnection(autoCommit, connection, JdbcSqlService.this);
            }
        };
    }

    private void setupHsqldb() throws InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, ClassNotFoundException {
        driver = loadedDrivers.get(dbName);
        if (driver == null) {
            driver = (Driver) Class.forName("org.hsqldb.jdbcDriver").getDeclaredConstructor().newInstance();
            loadedDrivers.put(dbName, driver);
        }
        if (namingMapping == DefaultNamingMapping.INSTANCE) {
            namingMapping = new NamingMapping() {
                public String getTableName(GlobType globType) {
                    return TargetTypeName.getOptName(globType).orElse(toSqlName(globType.getName()));
                }

                public String getColumnName(Field field) {
                    return DbFieldName.getOptName(field).orElse(toSqlName(field.getName()));
                }
            };
        }
        dbFactory = new DbFactory() {
            public JdbcConnection create(boolean autoCommit) {
                Connection connection = getConnection();
                try {
                    connection.setAutoCommit(autoCommit);
                } catch (SQLException e) {
                    throw new UnexpectedApplicationState(e);
                }
                return new HsqlConnection(autoCommit, connection, JdbcSqlService.this);
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

    private static class DefaultNamingMapping implements NamingMapping {
        public static NamingMapping INSTANCE = new DefaultNamingMapping();

        public String getTableName(GlobType globType) {
            return TargetTypeName.getOptName(globType).orElse(globType.getName());
        }

        public String getColumnName(Field field) {
            return DbFieldName.getOptName(field).orElse(field.getName());
        }
    }

    private static class ToLowerCaseNamingMapping implements NamingMapping {
        private NamingMapping namingMapping;

        public ToLowerCaseNamingMapping(NamingMapping namingMapping) {
            this.namingMapping = namingMapping;
        }

        public String getTableName(GlobType globType) {
            return namingMapping.getTableName(globType).toLowerCase(Locale.ROOT);
        }

        public String getColumnName(Field field) {
            return namingMapping.getColumnName(field).toLowerCase(Locale.ROOT);
        }
    }
}
