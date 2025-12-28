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
package com.facebook.presto.cassandra;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.internal.core.ssl.DefaultSslEngineFactory;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.plugin.base.security.SslContextProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import jakarta.inject.Singleton;

import javax.net.ssl.SSLContext;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.Optional;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class CassandraClientModule
        implements Module
{
    private final String connectorId;

    public CassandraClientModule(String connectorId)
    {
        this.connectorId = connectorId;
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(CassandraConnectorId.class).toInstance(new CassandraConnectorId(connectorId));
        binder.bind(CassandraConnector.class).in(Scopes.SINGLETON);
        binder.bind(CassandraSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(CassandraTokenSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(CassandraRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(CassandraPageSinkProvider.class).in(Scopes.SINGLETON);
        binder.bind(CassandraPartitionManager.class).in(Scopes.SINGLETON);
        binder.bind(CassandraSessionProperties.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(CassandraClientConfig.class);

        jsonCodecBinder(binder).bindListJsonCodec(ExtraColumnMetadata.class);
    }

    @Singleton
    @Provides
    public static CassandraSession createCassandraSession(
            CassandraConnectorId connectorId,
            CassandraClientConfig config,
            JsonCodec<List<ExtraColumnMetadata>> extraColumnMetadataCodec)
    {
        requireNonNull(config, "config is null");
        requireNonNull(extraColumnMetadataCodec, "extraColumnMetadataCodec is null");

        return new NativeCassandraSession(
                connectorId.toString(),
                extraColumnMetadataCodec,
                new ReopeningSession(() -> buildSession(config)),
                config.getNoHostAvailableRetryTimeout(),
                config.isCaseSensitiveNameMatchingEnabled());
    }

    private static CqlSession buildSession(CassandraClientConfig config)
    {
        CqlSessionBuilder sessionBuilder = CqlSession.builder();

        // Check for cloud configuration first
        if (config.getCloudSecureConnectBundle().isPresent()) {
            // Cloud mode: Use secure connect bundle for DataStax Astra
            sessionBuilder.withCloudSecureConnectBundle(
                    config.getCloudSecureConnectBundle().get().toPath());

            // Authentication is required for Astra
            if (config.getUsername() == null || config.getPassword() == null) {
                throw new IllegalArgumentException(
                        "Username and password are required when using cloud secure connect bundle");
            }
            sessionBuilder.withAuthCredentials(config.getUsername(), config.getPassword());

            // Note: Contact points, datacenter, and SSL are configured automatically by the bundle
        }
        else {
            // Standard mode: Configure contact points and datacenter
            List<String> contactPoints = requireNonNull(config.getContactPoints(), "contactPoints is null");
            checkArgument(!contactPoints.isEmpty(), "empty contactPoints");

            int port = config.getNativeProtocolPort();
            for (String contactPoint : contactPoints) {
                sessionBuilder.addContactPoint(new InetSocketAddress(contactPoint, port));
            }

            // Local datacenter is required in driver 4.x
            if (config.isUseDCAware() && config.getDcAwareLocalDC() != null) {
                sessionBuilder.withLocalDatacenter(config.getDcAwareLocalDC());
            }
            else {
                throw new IllegalArgumentException(
                        "Local datacenter must be specified (cassandra.load-policy.dc-aware.local-dc). " +
                                "Set cassandra.load-policy.use-dc-aware=true and provide cassandra.load-policy.dc-aware.local-dc");
            }

            // Configure authentication if provided
            if (config.getUsername() != null && config.getPassword() != null) {
                sessionBuilder.withAuthCredentials(config.getUsername(), config.getPassword());
            }
        }

        // Build programmatic configuration
        ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder = DriverConfigLoader.programmaticBuilder();

        // Request configuration
        configLoaderBuilder.withDuration(
                DefaultDriverOption.REQUEST_TIMEOUT,
                Duration.ofMillis(config.getClientReadTimeout().toMillis()));

        configLoaderBuilder.withInt(
                DefaultDriverOption.REQUEST_PAGE_SIZE,
                config.getFetchSize());

        configLoaderBuilder.withString(
                DefaultDriverOption.REQUEST_CONSISTENCY,
                config.getConsistencyLevel().name());

        // Socket configuration
        configLoaderBuilder.withDuration(
                DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT,
                Duration.ofMillis(config.getClientConnectTimeout().toMillis()));

        // Reconnection policy (exponential backoff)
        configLoaderBuilder.withString(
                DefaultDriverOption.RECONNECTION_POLICY_CLASS,
                "ExponentialReconnectionPolicy");
        configLoaderBuilder.withDuration(
                DefaultDriverOption.RECONNECTION_BASE_DELAY,
                Duration.ofMillis(500));
        configLoaderBuilder.withDuration(
                DefaultDriverOption.RECONNECTION_MAX_DELAY,
                Duration.ofMillis(10000));

        // Retry policy
        configLoaderBuilder.withString(
                DefaultDriverOption.RETRY_POLICY_CLASS,
                config.getRetryPolicy().getPolicyClass());

        // Speculative execution
        if (config.getSpeculativeExecutionLimit() > 1) {
            configLoaderBuilder.withString(
                    DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
                    "ConstantSpeculativeExecutionPolicy");
            configLoaderBuilder.withInt(
                    DefaultDriverOption.SPECULATIVE_EXECUTION_MAX,
                    config.getSpeculativeExecutionLimit());
            configLoaderBuilder.withDuration(
                    DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY,
                    Duration.ofMillis(config.getSpeculativeExecutionDelay().toMillis()));
        }

        // Load balancing policy configuration
        // Driver 4.x uses DefaultLoadBalancingPolicy which combines the functionality
        // of RoundRobin, DCAware, and TokenAware policies from driver 3.x
        configLoaderBuilder.withString(
                DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                "DefaultLoadBalancingPolicy");

        // Token-aware routing is enabled by default in driver 4.x
        // Note: LOAD_BALANCING_POLICY_SHUFFLE_REPLICAS option doesn't exist in Driver 4.x
        // The DefaultLoadBalancingPolicy in Driver 4.x handles replica shuffling automatically
        // through its slow replica avoidance feature
        if (config.isUseTokenAware()) {
            // Token-aware routing is always enabled in DefaultLoadBalancingPolicy
            // No explicit configuration needed
        }

        // DC-aware settings
        if (config.isUseDCAware()) {
            if (config.getDcAwareUsedHostsPerRemoteDc() > 0) {
                configLoaderBuilder.withInt(
                        DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC,
                        config.getDcAwareUsedHostsPerRemoteDc());

                if (config.isDcAwareAllowRemoteDCsForLocal()) {
                    configLoaderBuilder.withBoolean(
                            DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS,
                            true);
                }
            }
        }

        // Protocol version (optional - auto-negotiation by default)
        if (config.getProtocolVersion().isPresent()) {
            String version = config.getProtocolVersion().get().toUpperCase();
            // Validate the protocol version
            if (!version.matches("V[345]")) {
                throw new IllegalArgumentException(
                        "Invalid protocol version: " + version + ". " +
                                "Valid values are V3, V4, or V5. " +
                                "If not specified, the driver will auto-negotiate the best version.");
            }
            configLoaderBuilder.withString(
                    DefaultDriverOption.PROTOCOL_VERSION,
                    version);
        }
        // else: Let the driver auto-negotiate (recommended)

        // White list (node filtering) is not supported in driver 4.x
        if (config.isUseWhiteList()) {
            throw new IllegalArgumentException(
                    "White list node filtering (cassandra.load-policy.use-white-list) is not supported " +
                            "in Cassandra Java Driver 4.x. This feature was removed during the driver upgrade from 3.x to 4.x. " +
                            "To filter nodes, consider using network topology configuration, datacenter-aware routing, " +
                            "or carefully selecting contact points to limit node discovery. " +
                            "For more information about load balancing in driver 4.x, see: " +
                            "https://apache.github.io/cassandra-java-driver/4.19.0/core/load_balancing/");
        }

        // SSL/TLS configuration (only for non-cloud mode)
        if (!config.getCloudSecureConnectBundle().isPresent() && config.isTlsEnabled()) {
            SslContextProvider sslContextProvider = new SslContextProvider(
                    config.getKeystorePath(),
                    config.getKeystorePassword(),
                    config.getTruststorePath(),
                    config.getTruststorePassword());

            Optional<SSLContext> sslContext = sslContextProvider.buildSslContext();
            if (sslContext.isPresent()) {
                configLoaderBuilder.withClass(
                        DefaultDriverOption.SSL_ENGINE_FACTORY_CLASS,
                        DefaultSslEngineFactory.class);
                // SSL context will be picked up from system properties or custom factory
                // For more advanced SSL configuration, a custom SslEngineFactory can be implemented
            }
        }

        // Apply the configuration
        sessionBuilder.withConfigLoader(configLoaderBuilder.build());

        // Register custom codecs
        // Driver 4.x removed built-in support for java.sql.Timestamp, so we register a custom codec
        sessionBuilder.addTypeCodecs(TimestampCodec.INSTANCE);

        // Build and return the session
        return sessionBuilder.build();
    }
}
