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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.NodeManager;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import javax.annotation.PreDestroy;
import javax.inject.Inject;

import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.util.Map;

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
        this.jedisPoolCache = CacheBuilder.newBuilder().build(CacheLoader.from(this::createConsumer));
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
        return jedisPoolCache.getUnchecked(host);
    }

    private JedisPool createConsumer(HostAddress hostAddress)
    {
        if (redisConnectorConfig.isTlsEnabled()) {
            // Truststore setup (server.crt)
            KeyStore truststore = null;
            if (redisConnectorConfig.getTruststore() != null) {
                try {
                    truststore = KeyStore.getInstance(KeyStore.getDefaultType());
                    try (InputStream truststoreInput = Files.newInputStream(redisConnectorConfig.getTruststore().toPath())) {
                        truststore.load(null, null);
                        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
                        X509Certificate certificate = (X509Certificate) certificateFactory.generateCertificate(truststoreInput);
                        truststore.setCertificateEntry("redis-server", certificate);
                    }
                }
                catch (KeyStoreException | IOException | CertificateException | NoSuchAlgorithmException e) {
                    throw new RuntimeException(e);
                }
            }
            // SSL context setup
            TrustManagerFactory trustManagerFactory;
            try {
                trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
                trustManagerFactory.init(truststore);
            }
            catch (KeyStoreException | NoSuchAlgorithmException e) {
                throw new RuntimeException(e);
            }

            SSLContext sslContext;
            try {
                sslContext = SSLContext.getInstance("TLS");
                sslContext.init(null, trustManagerFactory.getTrustManagers(), null);
            }
            catch (NoSuchAlgorithmException | KeyManagementException e) {
                throw new RuntimeException(e);
            }

            // Jedis pool configuration
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMinIdle(1);
            poolConfig.setMaxTotal(5);
            log.info("Creating new JedisPool for %s", hostAddress);
            return new JedisPool(poolConfig, hostAddress.getHostText(), hostAddress.getPort(),
                    toIntExact(redisConnectorConfig.getRedisConnectTimeout().toMillis()),
                    2000, 2000, redisConnectorConfig.getRedisUser(), redisConnectorConfig.getRedisPassword(),
                    redisConnectorConfig.getRedisDataBaseIndex(), null, true, sslContext.getSocketFactory(),
                    null, null);
        }

        log.info("Creating new JedisPool for %s", hostAddress);
        return new JedisPool(jedisPoolConfig, hostAddress.getHostText(), hostAddress.getPort(),
                toIntExact(redisConnectorConfig.getRedisConnectTimeout().toMillis()),
                2000, 2000, redisConnectorConfig.getRedisUser(), redisConnectorConfig.getRedisPassword(),
                redisConnectorConfig.getRedisDataBaseIndex(), null, false, null, null, null);
    }
}
