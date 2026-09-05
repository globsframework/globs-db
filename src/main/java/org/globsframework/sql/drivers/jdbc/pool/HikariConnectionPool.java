package org.globsframework.sql.drivers.jdbc.pool;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

import javax.sql.DataSource;
import java.util.Properties;

/**
 * The only class of this artifact that references HikariCP. Never load it without going through
 * {@link ConnectionPools}, which checks first that the library is on the classpath.
 */
class HikariConnectionPool {

    private HikariConnectionPool() {
    }

    static DataSource create(String url, Properties dbInfo, PoolConfig poolConfig, String driverClassName) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(url);
        if (driverClassName != null) {
            config.setDriverClassName(driverClassName);
        }
        if (dbInfo != null) {
            Properties properties = new Properties();
            properties.putAll(dbInfo);
            // user and password are first class in Hikari, the rest is passed to the driver
            Object user = properties.remove("user");
            Object password = properties.remove("password");
            if (user != null) {
                config.setUsername(user.toString());
            }
            if (password != null) {
                config.setPassword(password.toString());
            }
            config.setDataSourceProperties(properties);
        }
        config.setMaximumPoolSize(poolConfig.getMaxPoolSize());
        if (poolConfig.getMinIdle() >= 0) {
            config.setMinimumIdle(poolConfig.getMinIdle());
        }
        config.setConnectionTimeout(poolConfig.getConnectionTimeout().toMillis());
        config.setIdleTimeout(poolConfig.getIdleTimeout().toMillis());
        config.setMaxLifetime(poolConfig.getMaxLifetime().toMillis());
        config.setValidationTimeout(poolConfig.getValidationTimeout().toMillis());
        if (poolConfig.getPoolName() != null) {
            config.setPoolName(poolConfig.getPoolName());
        }
        // the SqlConnection layer decides per connection whether it is transactional
        config.setAutoCommit(false);
        return new HikariDataSource(config);
    }
}
