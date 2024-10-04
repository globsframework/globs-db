package org.globsframework.sql.drivers.jdbc;

import org.globsframework.core.utils.exceptions.UnexpectedApplicationState;
import org.globsframework.sql.drivers.hsqldb.HsqlConnection;
import org.globsframework.sql.drivers.mysql.MysqlConnection;
import org.globsframework.sql.drivers.postgresql.PostgresqlConnection;
import org.globsframework.sql.utils.AbstractSqlService;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

public class DataSourceSqlService extends AbstractSqlService {
    final DataSource dataSource;
    DbFactory dbFactory;

    public DataSourceSqlService(NamingMapping namingMapping, DataSource dataSource, DbType dbType) {
        super(namingMapping);
        this.dataSource = dataSource;

        dbFactory = switch (dbType) {
            case postgresql -> (autoCommit) -> {
                try {
                    Connection connection = dataSource.getConnection();
                    connection.setAutoCommit(autoCommit);
                    return new PostgresqlConnection(autoCommit, connection, DataSourceSqlService.this);
                } catch (SQLException e) {
                    throw new UnexpectedApplicationState(e);
                }
            };
            case hsqldb -> (autoCommit) -> {
                try {
                    Connection connection = dataSource.getConnection();
                    connection.setAutoCommit(autoCommit);
                    return new HsqlConnection(autoCommit, connection, DataSourceSqlService.this);
                } catch (SQLException e) {
                    throw new UnexpectedApplicationState(e);
                }
            };
            case mysql, mariadb -> (autoCommit) -> {
                try {
                    Connection connection = dataSource.getConnection();
                    connection.setAutoCommit(autoCommit);
                    return new MysqlConnection(autoCommit, connection, DataSourceSqlService.this);
                } catch (SQLException e) {
                    throw new UnexpectedApplicationState(e);
                }
            };
        };
    }

    interface DbFactory {
        JdbcConnection create(boolean autoCommit);
    }

    public JdbcConnection getDb() {
        return dbFactory.create(false);
    }

    public JdbcConnection getAutoCommitDb() {
        return dbFactory.create(true);
    }
}
