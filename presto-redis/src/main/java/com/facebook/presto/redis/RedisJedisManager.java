/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.redis;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.PreDestroy;
import javax.inject.Inject;
import javax.inject.Named;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Manages connections to the Redis nodes
 */
public class RedisJedisManager
{
    private static final Logger log = Logger.get(RedisJedisManager.class);

    private final LoadingCache<HostAddress, JedisPool> jedisPoolCache;

    private final String connectorId;
    private final RedisConnectorConfig redisConnectorConfig;
    private final NodeManager nodeManager;
    private final JedisPoolConfig jedisPoolConfig;

    @Inject
    RedisJedisManager(@Named("connectorId") String connectorId,
                      RedisConnectorConfig redisConnectorConfig,
                      NodeManager nodeManager)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null");
        this.redisConnectorConfig = checkNotNull(redisConnectorConfig, "redisConfig is null");
        this.nodeManager = checkNotNull(nodeManager, "nodeManager is null");
        this.jedisPoolCache = CacheBuilder.newBuilder().build(new JedisPoolCacheLoader());
        this.jedisPoolConfig = new JedisPoolConfig();
    }

    @PreDestroy
    public void tearDown()
    {
        for (Map.Entry<HostAddress, JedisPool> entry : jedisPoolCache.asMap().entrySet()) {
            try {
                entry.getValue().destroy();
            }
            catch (Exception e) {
                log.warn(e, "While destroying JedisPool %s:", entry.getKey());
            }
        }
    }

    public NodeManager getNodeManager()
    {
        return nodeManager;
    }

    public RedisConnectorConfig getRedisConnectorConfig()
    {
        return redisConnectorConfig;
    }

    public JedisPool getJedisPool(HostAddress host)
    {
        checkNotNull(host, "host is null");
        try {
            return jedisPoolCache.get(host);
        }
        catch (ExecutionException e) {
            throw Throwables.propagate(e.getCause());
        }
    }

    private class JedisPoolCacheLoader
            extends CacheLoader<HostAddress, JedisPool>
    {
        @Override
        public JedisPool load(HostAddress host)
                throws Exception
        {
            log.info("Creating new JedisPool for %s", host);
            return new JedisPool(jedisPoolConfig,
                    host.getHostText(),
                    host.getPort(),
                    Ints.checkedCast(redisConnectorConfig.getRedisConnectTimeout().toMillis()),
                    redisConnectorConfig.getRedisPassword(),
                    redisConnectorConfig.getRedisDataBaseIndex());
        }
    }
}
