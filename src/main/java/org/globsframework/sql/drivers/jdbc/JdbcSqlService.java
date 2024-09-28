package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.metamodel.GlobType;
import org.globsframework.core.metamodel.fields.Field;
import org.globsframework.core.utils.exceptions.ItemNotFound;
import org.globsframework.core.utils.exceptions.UnexpectedApplicationState;
import org.globsframework.sql.annotations.DbFieldName;
import org.globsframework.sql.annotations.DbTableName;
import org.globsframework.sql.drivers.hsqldb.HsqlConnection;
import org.globsframework.sql.drivers.mysql.MysqlConnection;
import org.globsframework.sql.drivers.postgresql.PostgresqlConnection;
import org.globsframework.sql.drivers.postgresql.ToPostgreCaseNamingMapping;
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
        default String getTableName(GlobType globType, boolean escaped) {
            return getTableName(DbTableName.getOptName(globType).orElse(globType.getName()), escaped);
        }

        String getTableName(String typeName, boolean escaped);

        default String getColumnName(Field field, boolean escaped) {
            return getColumnName(DbFieldName.getOptName(field).orElse(field.getName()), escaped);
        }

        String getColumnName(String fieldName, boolean escaped);

        default String getLikeIgnoreCase() {
            return null;
        }
    }

    interface DbFactory {
        JdbcConnection create(boolean autoCommit);
    }

    public NamingMapping getNamingMapping() {
        return namingMapping;
    }

    public String getTableName(GlobType globType, boolean escaped) {
        return namingMapping.getTableName(globType, escaped);
    }

    public String getTableName(String name, boolean escaped) {
        return namingMapping.getTableName(name, escaped);
    }

    public String getColumnName(String field, boolean escaped) {
        return namingMapping.getColumnName(field, escaped);
    }

    public String getLikeIgnoreCase() {
        return namingMapping.getLikeIgnoreCase();
    }

    public String getColumnName(Field field, boolean escaped) {
        return namingMapping.getColumnName(field, escaped);
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
        namingMapping = new ToPostgreCaseNamingMapping(namingMapping) {
            public String getLikeIgnoreCase() {
                return "iLike";
            }
        };
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
        DataSource dataSource = (DataSource) Class.forName("org.hsqldb.jdbc.JDBCPool").getDeclaredConstructor().newInstance();
        dataSource.getClass().getDeclaredMethod("setUrl", String.class).invoke(dataSource, dbName);
        dataSource.getClass().getDeclaredMethod("setProperties", Properties.class).invoke(dataSource, dbInfo);
        if (namingMapping == DefaultNamingMapping.INSTANCE) {
            namingMapping = new NamingMapping() {
                public String getTableName(String typeName, boolean escaped) {
                    return toSqlName(typeName);
                }

                public String getColumnName(String fieldName, boolean escaped) {
                    return toSqlName(fieldName);
                }
            };
        }
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

    private static class DefaultNamingMapping implements NamingMapping {
        public static NamingMapping INSTANCE = new DefaultNamingMapping();

        public String getTableName(String typeName, boolean escaped) {
            return typeName;
        }

        public String getColumnName(String fieldName, boolean escaped) {
            return fieldName;
        }
    }

}
