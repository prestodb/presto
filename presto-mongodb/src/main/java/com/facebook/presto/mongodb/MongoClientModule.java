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
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;

import javax.inject.Singleton;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

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

        MongoClientSettings.Builder options = MongoClientSettings.builder();

        options.writeConcern(config.getWriteConcern().getWriteConcern())
                .readPreference(config.getReadPreference().getReadPreference())
                .applyToConnectionPoolSettings(builder -> builder
                        .maxWaitTime(config.getMaxWaitTime(), MILLISECONDS)
                        .minSize(config.getMinConnectionsPerHost())
                        .maxSize(config.getConnectionsPerHost()))
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(config.getConnectionTimeout(), MILLISECONDS)
                        .readTimeout(config.getSocketTimeout(), MILLISECONDS))
                .applyToSslSettings(builder -> builder.enabled(config.getSslEnabled()));

        if (config.getRequiredReplicaSetName() != null) {
            options.applyToClusterSettings(builder -> builder.requiredReplicaSetName(config.getRequiredReplicaSetName()));
        }

        if (config.getReadPreferenceTags().isEmpty()) {
            options.readPreference(config.getReadPreference().getReadPreference());
        }
        else {
            options.readPreference(config.getReadPreference().getReadPreferenceWithTags(config.getReadPreferenceTags()));
        }

        MongoClient client = MongoClients.create(options.build());

        return new MongoSession(
                typeManager,
                client,
                config);
    }
}
