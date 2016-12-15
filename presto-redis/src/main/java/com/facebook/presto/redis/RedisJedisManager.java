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
import io.airlift.log.Logger;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

/**
 * Manages connections to the Redis nodes
 */
public class RedisJedisManager
{
    private static final Logger log = Logger.get(RedisJedisManager.class);

    private final LoadingCache<HostAddress, JedisPool> jedisPoolCache;

    private final RedisConnectorConfig redisConnectorConfig;
    private final JedisPoolConfig jedisPoolConfig;

    @Inject
    RedisJedisManager(
            RedisConnectorConfig redisConnectorConfig,
            NodeManager nodeManager)
    {
        this.redisConnectorConfig = requireNonNull(redisConnectorConfig, "redisConfig is null");
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

    public RedisConnectorConfig getRedisConnectorConfig()
    {
        return redisConnectorConfig;
    }

    public JedisPool getJedisPool(HostAddress host)
    {
        requireNonNull(host, "host is null");
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
                    toIntExact(redisConnectorConfig.getRedisConnectTimeout().toMillis()),
                    redisConnectorConfig.getRedisPassword(),
                    redisConnectorConfig.getRedisDataBaseIndex());
        }
    }
}
