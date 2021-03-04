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
package com.facebook.presto.plugin.bigquery;

import com.facebook.presto.spi.NodeManager;
import com.google.api.gax.rpc.FixedHeaderProvider;
import com.google.api.gax.rpc.HeaderProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import com.google.cloud.bigquery.BigQueryOptions;
import com.google.inject.Binder;
import com.google.inject.Module;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

import java.util.Optional;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;

public class BigQueryConnectorModule
        implements Module
{
    private final NodeManager nodeManager;

    public BigQueryConnectorModule(NodeManager nodeManager)
    {
        this.nodeManager = nodeManager;
    }

    @Provides
    @Singleton
    public static HeaderProvider createHeaderProvider(NodeManager nodeManager)
    {
        return FixedHeaderProvider.create("user-agent", "prestodb/" + nodeManager.getCurrentNode().getVersion());
    }

    @Override
    public void configure(Binder binder)
    {
        binder.bind(BigQueryStorageClientFactory.class).in(Scopes.SINGLETON);
        binder.bind(BigQueryConnector.class).in(Scopes.SINGLETON);
        binder.bind(BigQueryMetadata.class).in(Scopes.SINGLETON);
        binder.bind(BigQuerySplitManager.class).in(Scopes.SINGLETON);
        binder.bind(BigQueryPageSourceProvider.class).in(Scopes.SINGLETON);

        configBinder(binder).bindConfig(BigQueryConfig.class);
    }

    @Provides
    @Singleton
    public BigQueryCredentialsSupplier provideBigQueryCredentialsSupplier(BigQueryConfig config)
    {
        return new BigQueryCredentialsSupplier(config.getCredentialsKey(), config.getCredentialsFile());
    }

    @Provides
    @Singleton
    public BigQueryClient provideBigQueryClient(BigQueryConfig config, HeaderProvider headerProvider, BigQueryCredentialsSupplier bigQueryCredentialsSupplier)
    {
        String billingProjectId = calculateBillingProjectId(config.getParentProjectId(), bigQueryCredentialsSupplier.getCredentials());
        BigQueryOptions.Builder options = BigQueryOptions.newBuilder()
                .setHeaderProvider(headerProvider)
                .setProjectId(billingProjectId);
        // set credentials if provided
        bigQueryCredentialsSupplier.getCredentials().ifPresent(options::setCredentials);
        return new BigQueryClient(options.build().getService(), config);
    }

    static String calculateBillingProjectId(Optional<String> configParentProjectId, Optional<Credentials> credentials)
    {
        if (configParentProjectId.isPresent()) {
            return configParentProjectId.get();
        }

        // All other credentials types (User, AppEngine, GCE, CloudShell, etc.) take it from the environment
        if (credentials.isPresent() && credentials.get() instanceof ServiceAccountCredentials) {
            return ((ServiceAccountCredentials) credentials.get()).getProjectId();
        }

        return BigQueryOptions.getDefaultProjectId();
    }
}
