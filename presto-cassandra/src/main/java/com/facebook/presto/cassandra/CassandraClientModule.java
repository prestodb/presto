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
import com.datastax.oss.driver.internal.core.loadbalancing.DefaultLoadBalancingPolicy;
import com.facebook.airlift.json.JsonCodec;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.plugin.base.security.SslContextProvider;
import com.facebook.presto.spi.PrestoException;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import jakarta.inject.Singleton;

import java.io.File;
import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.airlift.json.JsonCodecBinder.jsonCodecBinder;
import static com.facebook.presto.cassandra.CassandraErrorCode.CASSANDRA_ERROR;
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

        Logger log = Logger.get(CassandraClientModule.class);

        ReopeningSession reopeningSession = new ReopeningSession(() -> {
            CqlSessionBuilder sessionBuilder = CqlSession.builder();

            // Secure Connect Bundle (Astra) or Standard Configuration
            if (config.getSecureConnectBundle().isPresent()) {
                File bundlePath = config.getSecureConnectBundle().get();
                if (!bundlePath.exists()) {
                    throw new PrestoException(CASSANDRA_ERROR,
                            "Secure connect bundle not found: " + bundlePath);
                }

                log.info("Using secure connect bundle for Astra: %s", bundlePath);
                sessionBuilder.withCloudSecureConnectBundle(bundlePath.toPath());

                // Warn if contact points are specified (they will be ignored)
                if (!config.getContactPoints().isEmpty()) {
                    log.warn("Contact points ignored when using secure connect bundle: %s",
                            String.join(", ", config.getContactPoints()));
                }
            }
            else {
                // Standard Cassandra Configuration
                List<String> contactPoints = requireNonNull(config.getContactPoints(), "contactPoints is null");
                checkArgument(!contactPoints.isEmpty(), "empty contactPoints");

                List<InetSocketAddress> contactPointAddresses = contactPoints.stream()
                        .map(host -> new InetSocketAddress(host, config.getNativeProtocolPort()))
                        .collect(Collectors.toList());

                sessionBuilder.addContactPoints(contactPointAddresses);

                // Local datacenter is REQUIRED in driver 4.x
                String localDc = config.getDcAwareLocalDC();
                if (localDc == null || localDc.trim().isEmpty()) {
                    throw new PrestoException(CASSANDRA_ERROR,
                            "Local datacenter must be specified using 'cassandra.load-policy.dc-aware.local-dc' property");
                }
                sessionBuilder.withLocalDatacenter(localDc);
            }

            // Authentication (works for both Astra and standard Cassandra)
            if (config.getUsername() != null && config.getPassword() != null) {
                sessionBuilder.withAuthCredentials(config.getUsername(), config.getPassword());
            }

            // Build driver configuration
            ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder = DriverConfigLoader.programmaticBuilder();

            // Request timeout
            configLoaderBuilder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
                    Duration.ofMillis(config.getClientReadTimeout().toMillis()));

            // Connection timeout
            configLoaderBuilder.withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT,
                    Duration.ofMillis(config.getClientConnectTimeout().toMillis()));

            // Consistency level
            configLoaderBuilder.withString(DefaultDriverOption.REQUEST_CONSISTENCY,
                    config.getConsistencyLevel().name());

            // Page size
            configLoaderBuilder.withInt(DefaultDriverOption.REQUEST_PAGE_SIZE,
                    config.getFetchSize());

            // Reconnection policy
            configLoaderBuilder.withString(DefaultDriverOption.RECONNECTION_POLICY_CLASS,
                    "ExponentialReconnectionPolicy");
            configLoaderBuilder.withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY,
                    Duration.ofMillis(500));
            configLoaderBuilder.withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY,
                    Duration.ofMillis(10000));

            // Retry policy
            configLoaderBuilder.withClass(DefaultDriverOption.RETRY_POLICY_CLASS,
                    config.getRetryPolicy().getPolicyClass());

            // Load balancing policy
            configLoaderBuilder.withClass(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                    DefaultLoadBalancingPolicy.class);

            // Token awareness
            // In driver 4.x, token awareness is configured differently
            // The DefaultLoadBalancingPolicy already includes token awareness by default
            // We can configure it through the policy class if needed
            if (config.isUseTokenAware()) {
                // Token awareness is enabled by default in driver 4.x with DefaultLoadBalancingPolicy
                log.info("Token-aware load balancing is enabled (default in driver 4.x)");
            }

            // DC-aware settings
            if (config.getDcAwareUsedHostsPerRemoteDc() > 0) {
                configLoaderBuilder.withInt(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC,
                        config.getDcAwareUsedHostsPerRemoteDc());
                if (config.isDcAwareAllowRemoteDCsForLocal()) {
                    configLoaderBuilder.withBoolean(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS,
                            true);
                }
            }

            // Speculative execution
            if (config.getSpeculativeExecutionLimit() > 1) {
                configLoaderBuilder.withString(DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
                        "ConstantSpeculativeExecutionPolicy");
                configLoaderBuilder.withInt(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX,
                        config.getSpeculativeExecutionLimit());
                configLoaderBuilder.withDuration(DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY,
                        Duration.ofMillis(config.getSpeculativeExecutionDelay().toMillis()));
            }

            // SO_LINGER
            if (config.getClientSoLinger() != null) {
                configLoaderBuilder.withInt(DefaultDriverOption.SOCKET_LINGER_INTERVAL,
                        config.getClientSoLinger());
            }

            // Schema metadata configuration - include system keyspaces
            // By default, driver 4.x filters out system keyspaces, but we need them for size estimates
            configLoaderBuilder.withStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES,
                    java.util.Collections.emptyList()); // Empty list means include all keyspaces

            sessionBuilder.withConfigLoader(configLoaderBuilder.build());

            // TLS (only for non-Astra, as Astra bundle includes SSL config)
            if (!config.getSecureConnectBundle().isPresent() && config.isTlsEnabled()) {
                SslContextProvider sslContextProvider = new SslContextProvider(
                        config.getKeystorePath(),
                        config.getKeystorePassword(),
                        config.getTruststorePath(),
                        config.getTruststorePassword());

                sslContextProvider.buildSslContext().ifPresent(sslContext ->
                        sessionBuilder.withSslContext(sslContext));
            }

            return sessionBuilder.build();
        });

        return new NativeCassandraSession(
                connectorId.toString(),
                extraColumnMetadataCodec,
                reopeningSession,
                config.getNoHostAvailableRetryTimeout(),
                config.isCaseSensitiveNameMatchingEnabled());
    }
}
