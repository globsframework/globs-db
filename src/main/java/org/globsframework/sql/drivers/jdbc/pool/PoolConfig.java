package org.globsframework.sql.drivers.jdbc.pool;

import java.time.Duration;

/**
 * Sizing and timeouts of the connection pool used by {@code JdbcSqlService}. Immutable: every
 * {@code with*} returns a copy, so {@link #DEFAULT} can be shared.
 * <p>
 * The values map onto the underlying pool implementation (HikariCP today) and keep its defaults
 * where they are already sensible.
 */
public class PoolConfig {
    public static final PoolConfig DEFAULT = new PoolConfig();

    /**
     * Opt out of pooling: one physical connection per getDb(), the behaviour of this library before
     * pooling existed.
     */
    public static final PoolConfig NO_POOL = new PoolConfig().withPooling(false);

    private boolean pooling = true;
    private int maxPoolSize = 10;
    private int minIdle = -1;
    private Duration connectionTimeout = Duration.ofSeconds(30);
    private Duration idleTimeout = Duration.ofMinutes(10);
    private Duration maxLifetime = Duration.ofMinutes(30);
    private Duration validationTimeout = Duration.ofSeconds(5);
    private String poolName;

    public PoolConfig() {
    }

    private PoolConfig(PoolConfig other) {
        this.pooling = other.pooling;
        this.maxPoolSize = other.maxPoolSize;
        this.minIdle = other.minIdle;
        this.connectionTimeout = other.connectionTimeout;
        this.idleTimeout = other.idleTimeout;
        this.maxLifetime = other.maxLifetime;
        this.validationTimeout = other.validationTimeout;
        this.poolName = other.poolName;
    }

    public PoolConfig withPooling(boolean pooling) {
        PoolConfig copy = new PoolConfig(this);
        copy.pooling = pooling;
        return copy;
    }

    public PoolConfig withMaxPoolSize(int maxPoolSize) {
        PoolConfig copy = new PoolConfig(this);
        copy.maxPoolSize = maxPoolSize;
        return copy;
    }

    /**
     * Connections kept open while idle. Negative means "same as maxPoolSize", a fixed size pool.
     */
    public PoolConfig withMinIdle(int minIdle) {
        PoolConfig copy = new PoolConfig(this);
        copy.minIdle = minIdle;
        return copy;
    }

    /**
     * How long getDb() waits for a free connection before failing.
     */
    public PoolConfig withConnectionTimeout(Duration connectionTimeout) {
        PoolConfig copy = new PoolConfig(this);
        copy.connectionTimeout = connectionTimeout;
        return copy;
    }

    public PoolConfig withIdleTimeout(Duration idleTimeout) {
        PoolConfig copy = new PoolConfig(this);
        copy.idleTimeout = idleTimeout;
        return copy;
    }

    public PoolConfig withMaxLifetime(Duration maxLifetime) {
        PoolConfig copy = new PoolConfig(this);
        copy.maxLifetime = maxLifetime;
        return copy;
    }

    public PoolConfig withValidationTimeout(Duration validationTimeout) {
        PoolConfig copy = new PoolConfig(this);
        copy.validationTimeout = validationTimeout;
        return copy;
    }

    /**
     * Name reported by the pool in logs and metrics.
     */
    public PoolConfig withPoolName(String poolName) {
        PoolConfig copy = new PoolConfig(this);
        copy.poolName = poolName;
        return copy;
    }

    public boolean isPooling() {
        return pooling;
    }

    public int getMaxPoolSize() {
        return maxPoolSize;
    }

    public int getMinIdle() {
        return minIdle;
    }

    public Duration getConnectionTimeout() {
        return connectionTimeout;
    }

    public Duration getIdleTimeout() {
        return idleTimeout;
    }

    public Duration getMaxLifetime() {
        return maxLifetime;
    }

    public Duration getValidationTimeout() {
        return validationTimeout;
    }

    public String getPoolName() {
        return poolName;
    }

    public String toString() {
        return "PoolConfig{pooling=" + pooling + ", maxPoolSize=" + maxPoolSize + ", minIdle=" + minIdle
               + ", connectionTimeout=" + connectionTimeout + ", idleTimeout=" + idleTimeout
               + ", maxLifetime=" + maxLifetime + ", poolName=" + poolName + "}";
    }
}
