package org.globsframework.sqlstreams.drivers.jdbc;

import org.globsframework.metamodel.Field;
import org.globsframework.metamodel.GlobType;
import org.globsframework.sqlstreams.SqlConnection;
import org.globsframework.sqlstreams.drivers.hsqldb.HsqlConnection;
import org.globsframework.sqlstreams.drivers.mysql.MysqlConnection;
import org.globsframework.sqlstreams.utils.AbstractSqlService;
import org.globsframework.utils.exceptions.ItemNotFound;
import org.globsframework.utils.exceptions.UnexpectedApplicationState;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class JdbcSqlService extends AbstractSqlService {
    private static Map<String, Driver> loadedDrivers = new HashMap<String, Driver>();
    private Driver driver;
    private String dbName;
    private Properties dbInfo;
    private DbFactory dbFactory;
    NamingMapping namingMapping;

    public JdbcSqlService(String dbName, String user, String password, NamingMapping namingMapping) {
        this.dbName = dbName;
        this.namingMapping = namingMapping;
        dbInfo = new Properties();
        dbInfo.put("user", user);
        dbInfo.put("password", password);
        loadDriver();
    }

    public JdbcSqlService(String dbName, String user, String password) {
        this(dbName, user, password, new NamingMapping() {
            public String getTableName(GlobType globType) {
                return AbstractSqlService.toSqlName(globType.getName());
            }

            public String getColumnName(Field field) {
                return AbstractSqlService.toSqlName(field.getName());
            }
        });
    }

    public interface NamingMapping {
        String getTableName(GlobType globType);

        String getColumnName(Field field);
    }

    interface DbFactory {
        SqlConnection create();
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
                if (!loadedDrivers.containsKey("hsqldb")) {
                    driver = (Driver) Class.forName("org.hsqldb.jdbcDriver").newInstance();
                }
                dbFactory = new DbFactory() {
                    public SqlConnection create() {
                        Connection connection = getConnection();
                        try {
                            connection.setAutoCommit(false);
                        } catch (SQLException e) {
                            throw new UnexpectedApplicationState(e);
                        }
                        return new HsqlConnection(connection, JdbcSqlService.this);
                    }
                };
            } else if (dbName.contains("mysql")) {
                if (!loadedDrivers.containsKey("mysdb")) {
                    driver = (Driver) Class.forName("com.mysql.jdbc.Driver").newInstance();
                }
//    dbInfo.put("autoReconnect", Boolean.TRUE);
                dbFactory = new DbFactory() {
                    public SqlConnection create() {
                        Connection connection = getConnection();
                        try {
                            connection.setAutoCommit(false);
                        } catch (SQLException e) {
                            throw new UnexpectedApplicationState(e);
                        }

                        return new MysqlConnection(connection, JdbcSqlService.this);
                    }
                };
            }
        } catch (Exception e) {
            throw new ItemNotFound(e);
        }
    }

    public SqlConnection getDb() {
        return dbFactory.create();
    }

    synchronized public Connection getConnection() {
        try {
            return driver.connect(dbName, dbInfo);
        } catch (SQLException e) {
            throw new UnexpectedApplicationState("for " + dbInfo.get("user") + " on " + dbName, e);
        }
    }
}
