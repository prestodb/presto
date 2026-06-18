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
package com.facebook.presto.mongodb;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.plugin.base.security.SslContextProvider;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import jakarta.inject.Singleton;

import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;

public class MongoClientModule
        implements Module
{
    @Override
    public void configure(Binder binder)
    {
        binder.bind(MongoConnector.class).in(Scopes.SINGLETON);
        binder.bind(MongoSplitManager.class).in(Scopes.SINGLETON);
        binder.bind(MongoPageSourceProvider.class).in(Scopes.SINGLETON);
        binder.bind(MongoPageSinkProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(MongoClientConfig.class);
    }

    @Singleton
    @Provides
    public static MongoSession createMongoSession(TypeManager typeManager, MongoClientConfig config)
    {
        requireNonNull(config, "config is null");

        MongoClientSettings.Builder settingsBuilder = MongoClientSettings.builder();

        String connectionString = buildConnectionString(config);
        settingsBuilder.applyConnectionString(new ConnectionString(connectionString));

        config.getCredentials().ifPresent(settingsBuilder::credential);

        settingsBuilder.applyToConnectionPoolSettings(builder -> builder
                .maxSize(config.getConnectionsPerHost())
                .minSize(config.getMinConnectionsPerHost())
                .maxWaitTime(config.getMaxWaitTime(), TimeUnit.MILLISECONDS));

        settingsBuilder.applyToSocketSettings(builder -> builder
                .connectTimeout(config.getConnectionTimeout(), TimeUnit.MILLISECONDS)
                .readTimeout(config.getSocketTimeout(), TimeUnit.MILLISECONDS));

        settingsBuilder.writeConcern(config.getWriteConcern().getWriteConcern());

        settingsBuilder.readPreference(configureReadPreference(config));

        if (config.getRequiredReplicaSetName() != null) {
            settingsBuilder.applyToClusterSettings(builder ->
                    builder.requiredReplicaSetName(config.getRequiredReplicaSetName()));
        }

        configureSsl(settingsBuilder, config);

        MongoClient client = MongoClients.create(settingsBuilder.build());

        return new MongoSession(typeManager, client, config);
    }

    private static String buildConnectionString(MongoClientConfig config)
    {
        StringBuilder connectionString = new StringBuilder("mongodb://");

        connectionString.append(config.getSeeds().stream()
                .map(addr -> addr.getHost() + ":" + addr.getPort())
                .collect(Collectors.joining(",")));

        config.getCredentials().ifPresent(credential ->
                connectionString.append("/")
                        .append(credential.getSource()));

        // Enable replica set discovery when replica set name is configured or multiple seeds are provided
        if (config.getRequiredReplicaSetName() != null || config.getSeeds().size() > 1) {
            connectionString.append("?directConnection=false");
        }
        return connectionString.toString();
    }
    private static ReadPreference configureReadPreference(MongoClientConfig config)
    {
        if (config.getReadPreferenceTags().isEmpty()) {
            return config.getReadPreference().getReadPreference();
        }
        else {
            return config.getReadPreference().getReadPreferenceWithTags(config.getReadPreferenceTags());
        }
    }

    private static void configureSsl(MongoClientSettings.Builder settings, MongoClientConfig config)
    {
        if (config.isTlsEnabled()) {
            SslContextProvider sslContextProvider = new SslContextProvider(
                    config.getKeystorePath(),
                    config.getKeystorePassword(),
                    config.getTruststorePath(),
                    config.getTruststorePassword());

            sslContextProvider.buildSslContext()
                    .ifPresent(sslContext -> {
                        settings.applyToSslSettings(builder -> builder
                                .enabled(true)
                                .context(sslContext));
                    });
        }
    }
}
