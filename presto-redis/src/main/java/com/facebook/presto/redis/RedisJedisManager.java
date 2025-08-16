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
import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

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
import static javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm;

/**
 * Manages connections to the Redis nodes
 */
public class RedisJedisManager
{
    private static final Logger log = Logger.get(RedisJedisManager.class);

    private static final String TLS_PROTOCOL = "TLS";
    private static final int JEDIS_CONN_TIMEOUT = 2000;
    private static final int JEDIS_SO_TIMEOUT = 2000;
    private static final int JEDIS_MIN_IDLE_CONNECTIONS = 1;
    private static final int JEDIS_MAX_IDLE_CONNECTIONS = 5;

    private final LoadingCache<HostAddress, JedisPool> jedisPoolCache;

    private final RedisConnectorConfig redisConnectorConfig;
    private final JedisPoolConfig jedisPoolConfig;

    @Inject
    RedisJedisManager(
            RedisConnectorConfig redisConnectorConfig,
            NodeManager nodeManager)
    {
        this.redisConnectorConfig = requireNonNull(redisConnectorConfig, "redisConfig is null");
        this.jedisPoolCache = CacheBuilder.newBuilder().build(CacheLoader.from(this::createJedisPool));
        this.jedisPoolConfig = createJedisPoolConfig();
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

    /**
     * Creates a new JedisPool for the specified host.
     * Chooses between TLS or non-TLS configuration based on redisConnectorConfig.
     */
    private JedisPool createJedisPool(HostAddress host)
    {
        boolean isTlsEnabled = redisConnectorConfig.isTlsEnabled();
        SSLContext sslContext = null;

        if (isTlsEnabled) {
            KeyStore trustStore = loadTrustStore();
            sslContext = createSslContext(trustStore);
        }

        return buildJedisPool(host, isTlsEnabled, sslContext);
    }

    /**
     * Creates SSLContext initialized with the given truststore.
     */
    private SSLContext createSslContext(KeyStore trustStore)
    {
        if (trustStore == null) {
            throw new IllegalStateException("Truststore must not be null for TLS connections");
        }

        try {
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(getDefaultAlgorithm());
            tmf.init(trustStore);

            SSLContext sslContext = SSLContext.getInstance(TLS_PROTOCOL);
            sslContext.init(null, tmf.getTrustManagers(), null);

            return sslContext;
        }
        catch (NoSuchAlgorithmException | KeyStoreException | KeyManagementException e) {
            throw new RuntimeException("Failed to initialize SSLContext", e);
        }
    }

    private JedisPoolConfig createJedisPoolConfig()
    {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMinIdle(JEDIS_MIN_IDLE_CONNECTIONS);
        config.setMaxTotal(JEDIS_MAX_IDLE_CONNECTIONS);
        return config;
    }

    /**
     * Loads the truststore containing Redis server certificate.
     * Returns null if truststore path is not configured.
     */
    private KeyStore loadTrustStore()
    {
        if (redisConnectorConfig.getTruststorePath() == null) {
            log.info("No truststore path configured, skipping TLS truststore loading");
            return null;
        }

        try {
            KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
            try (InputStream in = Files.newInputStream(redisConnectorConfig.getTruststorePath().toPath())) {
                trustStore.load(null, null);
                CertificateFactory cf = CertificateFactory.getInstance("X.509");
                X509Certificate cert = (X509Certificate) cf.generateCertificate(in);
                trustStore.setCertificateEntry("redis-server", cert);
            }
            log.info("Loaded truststore from %s", redisConnectorConfig.getTruststorePath());
            return trustStore;
        }
        catch (KeyStoreException | IOException | CertificateException | NoSuchAlgorithmException e) {
            throw new RuntimeException("Failed to load truststore", e);
        }
    }

    private JedisPool buildJedisPool(HostAddress host, boolean useTls, SSLContext sslContext)
    {
        log.info("Creating new %s JedisPool for %s", useTls ? "TLS" : "non-TLS", host);

        return new JedisPool(
                jedisPoolConfig,
                host.getHostText(),
                host.getPort(),
                toIntExact(redisConnectorConfig.getRedisConnectTimeout().toMillis()),
                JEDIS_SO_TIMEOUT,
                JEDIS_CONN_TIMEOUT,
                redisConnectorConfig.getRedisUser(),
                redisConnectorConfig.getRedisPassword(),
                redisConnectorConfig.getRedisDataBaseIndex(),
                null,
                useTls,
                useTls ? sslContext.getSocketFactory() : null,
                null,
                null);
    }
}
