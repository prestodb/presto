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
import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;

import javax.inject.Singleton;

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

        MongoClientOptions.Builder options = MongoClientOptions.builder();

        options.connectionsPerHost(config.getConnectionsPerHost())
                .connectTimeout(config.getConnectionTimeout())
                .socketTimeout(config.getSocketTimeout())
                .socketKeepAlive(config.getSocketKeepAlive())
                .sslEnabled(config.getSslEnabled())
                .maxWaitTime(config.getMaxWaitTime())
                .minConnectionsPerHost(config.getMinConnectionsPerHost())
                .writeConcern(config.getWriteConcern().getWriteConcern());

        if (config.getRequiredReplicaSetName() != null) {
            options.requiredReplicaSetName(config.getRequiredReplicaSetName());
        }

        if (config.getReadPreferenceTags().isEmpty()) {
            options.readPreference(config.getReadPreference().getReadPreference());
        }
        else {
            options.readPreference(config.getReadPreference().getReadPreferenceWithTags(config.getReadPreferenceTags()));
        }

        MongoClient client = new MongoClient(config.getSeeds(), config.getCredentials(), options.build());

        return new MongoSession(
                typeManager,
                client,
                config);
    }
}
