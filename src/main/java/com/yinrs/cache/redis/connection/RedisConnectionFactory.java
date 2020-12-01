package com.yinrs.cache.redis.connection;


import com.yinrs.cache.redis.RedisNode;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.*;
import redis.clients.util.Pool;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * Connection factory creating <a href="http://github.com/xetorthio/jedis">Jedis</a> based connections.
 *
 * @author Costin Leau
 * @author Thomas Darimont
 * @author Christoph Strobl
 * @author Mark Paluch
 */
public class RedisConnectionFactory {

    private final static Logger log = LoggerFactory.getLogger(RedisConnectionFactory.class);

    private JedisShardInfo shardInfo;
    private String hostName = "localhost";
    private int port = Protocol.DEFAULT_PORT;
    private int timeout = Protocol.DEFAULT_TIMEOUT;
    private String password;
    private Pool<Jedis> pool;
    private JedisPoolConfig poolConfig = new JedisPoolConfig();
    private int dbIndex = 0;
    private JedisCluster cluster;
    private List<RedisNode> redisServers;
    private boolean isCluster = false;
    private String model = "single";
    protected String sentinelMaster = "mymaster";
    protected Set<String> sentinelSet = null;

    /**
     * Constructs a new <code>JedisConnectionFactory</code> instance with default settings (default connection pooling, no
     * shard information).
     */
    public RedisConnectionFactory() {
    }

    /**
     * Returns a Jedis instance to be used as a Redis connection. The instance can be newly created or retrieved from a
     * pool.
     */
    protected Jedis fetchJedisConnector() {
        try {

            if (pool != null) {
                return pool.getResource();
            }
            Jedis jedis = new Jedis(getShardInfo());
            // force initialization (see Jedis issue #82)
            jedis.connect();
            return jedis;
        } catch (Exception ex) {
            throw new RuntimeException("Cannot get Jedis connection", ex);
        }
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.beans.factory.InitializingBean#afterPropertiesSet()
     */
    public void init() {
        if (shardInfo == null) {
            shardInfo = new JedisShardInfo(hostName, port);
            if (StringUtils.isNotEmpty(password)) {
                shardInfo.setPassword(password);
            }
            if (timeout > 0) {
                shardInfo.setConnectionTimeout(timeout);
            }
        }

        if (model.equals("cluster")) {
            this.cluster = createCluster();
        } else if (model.equals("sentinel")) {
            this.pool = createSentinelPool();
        } else {
            this.pool = createPool();
        }
    }

    private Pool<Jedis> createPool() {
        return createRedisPool();
    }


    /**
     * Creates {@link JedisPool}.
     *
     * @return
     * @since 1.4
     */
    protected Pool<Jedis> createRedisPool() {
        return new JedisPool(getPoolConfig(), getShardInfo().getHost(), getShardInfo().getPort(),
                getShardInfo().getSoTimeout(), getShardInfo().getPassword());
    }


    private Pool<Jedis> createSentinelPool() {
        Set<String> sentinelSet = getSentinelSet();
        if (sentinelSet != null && sentinelSet.size() > 0) {
            return new JedisSentinelPool(getSentinelMaster(), sentinelSet, this.poolConfig, getShardInfo().getConnectionTimeout(), getShardInfo().getPassword());
        } else {
            throw new IllegalArgumentException(
                    "Error configuring Redis Sentinel connection pool: expected both `sentinelMaster` and `sentiels` to be configured");
        }
    }


    public Set<String> getSentinelSet() {
        Set<String> hostAndPort = new HashSet<String>();

        for (RedisNode server : this.redisServers) {
            hostAndPort.add(server.host + ":" + server.port);
        }
        return hostAndPort;
    }


    private JedisCluster createCluster() {
        return createCluster(this.redisServers, this.poolConfig);
    }

    /**
     * @param poolConfig can be {@literal null}.
     * @return
     * @since 1.7
     */
    protected JedisCluster createCluster(List<RedisNode> servers, GenericObjectPoolConfig poolConfig) {

        Set<HostAndPort> hostAndPort = new HashSet<>();
        for (RedisNode server : servers) {
            hostAndPort.add(new HostAndPort(server.host, server.port));
        }
        int redirects = 5;

        if (StringUtils.isNotEmpty(getPassword())) {
            throw new IllegalArgumentException("Jedis does not support password protected Redis Cluster configurations!");
        }

        if (poolConfig != null) {
            return new JedisCluster(hostAndPort, timeout, redirects, poolConfig);
        }
        return new JedisCluster(hostAndPort, timeout, redirects, poolConfig);
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.beans.factory.DisposableBean#destroy()
     */
    public void destroy() {
        if (pool != null) {
            try {
                pool.destroy();
            } catch (Exception ex) {
                log.warn("Cannot properly close Jedis pool", ex);
            }
            pool = null;
        }
        if (cluster != null) {
            try {
                cluster.close();
            } catch (Exception ex) {
                log.warn("Cannot properly close Jedis cluster", ex);
            }
            cluster = null;
        }
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.redis.connection.RedisConnectionFactory#getConnection()
     */
    public Jedis getJedisConnection() {
        return fetchJedisConnector();
    }

    /*
     * (non-Javadoc)
     * @see org.springframework.data.redis.connection.RedisConnectionFactory#getClusterConnection()
     */
    public JedisCluster getClusterConnection() {
        return cluster;
    }

    public boolean isCluster() {
        return isCluster;
    }

    public String getModel() {
        return model;
    }

    public void setModel(String model) {
        this.model = model;
    }

    public String getSentinelMaster() {
        return this.sentinelMaster;
    }

    public void setSentinelMaster(String master) {
        this.sentinelMaster = master;
    }

    /**
     * Returns the Redis hostName.
     *
     * @return Returns the hostName
     */
    public String getHostName() {
        return hostName;
    }

    /**
     * Sets the Redis hostName.
     *
     * @param hostName The hostName to set.
     */
    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    /**
     * Returns the password used for authenticating with the Redis server.
     *
     * @return password for authentication
     */
    public String getPassword() {
        return password;
    }

    /**
     * Sets the password used for authenticating with the Redis server.
     *
     * @param password the password to set
     */
    public void setPassword(String password) {
        this.password = password;
    }

    /**
     * Returns the port used to connect to the Redis instance.
     *
     * @return Redis port.
     */
    public int getPort() {
        return port;

    }

    /**
     * Sets the port used to connect to the Redis instance.
     *
     * @param port Redis port
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Returns the shardInfo.
     *
     * @return Returns the shardInfo
     */
    public JedisShardInfo getShardInfo() {
        return shardInfo;
    }

    /**
     * Sets the shard info for this factory.
     *
     * @param shardInfo The shardInfo to set.
     */
    public void setShardInfo(JedisShardInfo shardInfo) {
        this.shardInfo = shardInfo;
    }

    /**
     * Returns the timeout.
     *
     * @return Returns the timeout
     */
    public int getTimeout() {
        return timeout;
    }

    /**
     * @param timeout The timeout to set.
     */
    public void setTimeout(int timeout) {
        this.timeout = timeout;
    }

    /**
     * Returns the poolConfig.
     *
     * @return Returns the poolConfig
     */
    public JedisPoolConfig getPoolConfig() {
        return poolConfig;
    }

    /**
     * Sets the pool configuration for this factory.
     *
     * @param poolConfig The poolConfig to set.
     */
    public void setPoolConfig(JedisPoolConfig poolConfig) {
        this.poolConfig = poolConfig;
    }

    /**
     * Returns the index of the database.
     *
     * @return Returns the database index
     */
    public int getDatabase() {
        return dbIndex;
    }

    /**
     * Sets the index of the database used by this connection factory. Default is 0.
     *
     * @param index database index
     */
    public void setDatabase(int index) {
        this.dbIndex = index;
    }

    public void setCluster(boolean cluster) {
        isCluster = cluster;
    }

    public void setRedisServers(List<RedisNode> redisServers) {
        Objects.requireNonNull(redisServers);
        this.redisServers = redisServers;
        this.hostName = redisServers.get(0).getHost();
        this.port = redisServers.get(0).getPort();
    }
}