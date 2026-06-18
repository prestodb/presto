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
import java.util.Collections;
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
            configureEndpoint(sessionBuilder, config, log);
            configureAuth(sessionBuilder, config);
            sessionBuilder.withConfigLoader(buildDriverConfig(config));
            sessionBuilder.addTypeCodecs(new IntToLocalDateCodec());
            sessionBuilder.addTypeCodecs(TimestampCodec.INSTANCE);
            configureTls(sessionBuilder, config);
            return sessionBuilder.build();
        });

        return new NativeCassandraSession(
                connectorId.toString(),
                extraColumnMetadataCodec,
                reopeningSession,
                config.getNoHostAvailableRetryTimeout(),
                config.isCaseSensitiveNameMatchingEnabled(),
                config.getVectorMaxDimensions());
    }

    static void configureEndpoint(CqlSessionBuilder sessionBuilder, CassandraClientConfig config, Logger log)
    {
        if (config.getSecureConnectBundle().isPresent()) {
            File bundlePath = config.getSecureConnectBundle().get();
            if (!bundlePath.exists()) {
                throw new PrestoException(CASSANDRA_ERROR,
                        "Secure connect bundle not found: " + bundlePath);
            }
            log.info("Using secure connect bundle for Astra: %s", bundlePath);
            sessionBuilder.withCloudSecureConnectBundle(bundlePath.toPath());
            if (!config.getContactPoints().isEmpty()) {
                log.warn("Contact points ignored when using secure connect bundle: %s",
                        String.join(", ", config.getContactPoints()));
            }
        }
        else {
            List<String> contactPoints = requireNonNull(config.getContactPoints(), "contactPoints is null");
            checkArgument(!contactPoints.isEmpty(), "empty contactPoints");
            List<InetSocketAddress> contactPointAddresses = contactPoints.stream()
                    .map(host -> new InetSocketAddress(host, config.getNativeProtocolPort()))
                    .collect(Collectors.toList());
            sessionBuilder.addContactPoints(contactPointAddresses);
            String localDc = config.getDcAwareLocalDC();
            if (localDc == null || localDc.trim().isEmpty()) {
                throw new PrestoException(CASSANDRA_ERROR,
                        "Local datacenter must be specified using 'cassandra.load-policy.dc-aware.local-dc' property");
            }
            sessionBuilder.withLocalDatacenter(localDc);
        }
    }

    private static void configureAuth(CqlSessionBuilder sessionBuilder, CassandraClientConfig config)
    {
        if (config.getUsername() != null && config.getPassword() != null) {
            sessionBuilder.withAuthCredentials(config.getUsername(), config.getPassword());
        }
    }

    static DriverConfigLoader buildDriverConfig(CassandraClientConfig config)
    {
        ProgrammaticDriverConfigLoaderBuilder builder = DriverConfigLoader.programmaticBuilder();

        builder.withDuration(DefaultDriverOption.REQUEST_TIMEOUT,
                Duration.ofMillis(config.getClientReadTimeout().toMillis()));
        builder.withDuration(DefaultDriverOption.CONNECTION_CONNECT_TIMEOUT,
                Duration.ofMillis(config.getClientConnectTimeout().toMillis()));
        builder.withString(DefaultDriverOption.REQUEST_CONSISTENCY,
                config.getConsistencyLevel().name());
        builder.withInt(DefaultDriverOption.REQUEST_PAGE_SIZE,
                config.getFetchSize());
        config.getProtocolVersion().ifPresent(protocolVersion ->
                builder.withString(DefaultDriverOption.PROTOCOL_VERSION, protocolVersion));
        builder.withString(DefaultDriverOption.RECONNECTION_POLICY_CLASS,
                "ExponentialReconnectionPolicy");
        builder.withDuration(DefaultDriverOption.RECONNECTION_BASE_DELAY,
                Duration.ofMillis(500));
        builder.withDuration(DefaultDriverOption.RECONNECTION_MAX_DELAY,
                Duration.ofMillis(10000));
        builder.withString(DefaultDriverOption.RETRY_POLICY_CLASS,
                config.getRetryPolicy().getPolicyClassName());
        builder.withString(DefaultDriverOption.LOAD_BALANCING_POLICY_CLASS,
                "DefaultLoadBalancingPolicy");
        configureDcFailover(builder, config);
        configureSpeculativeExecution(builder, config);
        if (config.getClientSoLinger() != null) {
            builder.withInt(DefaultDriverOption.SOCKET_LINGER_INTERVAL,
                    config.getClientSoLinger());
        }
        builder.withStringList(DefaultDriverOption.METADATA_SCHEMA_REFRESHED_KEYSPACES,
                Collections.emptyList());

        return builder.build();
    }

    private static void configureDcFailover(ProgrammaticDriverConfigLoaderBuilder builder, CassandraClientConfig config)
    {
        if (config.getDcAwareUsedHostsPerRemoteDc() > 0) {
            builder.withInt(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_MAX_NODES_PER_REMOTE_DC,
                    config.getDcAwareUsedHostsPerRemoteDc());
            if (config.isDcAwareAllowRemoteDCsForLocal()) {
                builder.withBoolean(DefaultDriverOption.LOAD_BALANCING_DC_FAILOVER_ALLOW_FOR_LOCAL_CONSISTENCY_LEVELS,
                        true);
            }
        }
    }

    private static void configureSpeculativeExecution(ProgrammaticDriverConfigLoaderBuilder builder, CassandraClientConfig config)
    {
        if (config.getSpeculativeExecutionLimit() > 1) {
            builder.withString(DefaultDriverOption.SPECULATIVE_EXECUTION_POLICY_CLASS,
                    "ConstantSpeculativeExecutionPolicy");
            builder.withInt(DefaultDriverOption.SPECULATIVE_EXECUTION_MAX,
                    config.getSpeculativeExecutionLimit());
            builder.withDuration(DefaultDriverOption.SPECULATIVE_EXECUTION_DELAY,
                    Duration.ofMillis(config.getSpeculativeExecutionDelay().toMillis()));
        }
    }

    private static void configureTls(CqlSessionBuilder sessionBuilder, CassandraClientConfig config)
    {
        if (!config.getSecureConnectBundle().isPresent() && config.isTlsEnabled()) {
            SslContextProvider sslContextProvider = new SslContextProvider(
                    config.getKeystorePath(),
                    config.getKeystorePassword(),
                    config.getTruststorePath(),
                    config.getTruststorePassword());
            sslContextProvider.buildSslContext().ifPresent(sslContext ->
                    sessionBuilder.withSslContext(sslContext));
        }
    }
}
