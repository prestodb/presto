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
package com.facebook.presto.plugin.clp;

import com.facebook.airlift.configuration.AbstractConfigurationAwareModule;
import com.facebook.presto.plugin.clp.metadata.ClpMetadataProvider;
import com.facebook.presto.plugin.clp.metadata.ClpMySqlMetadataProvider;
import com.facebook.presto.plugin.clp.split.ClpMySqlSplitProvider;
import com.facebook.presto.plugin.clp.split.ClpSplitProvider;
import com.facebook.presto.plugin.clp.split.filter.ClpMySqlSplitFilterProvider;
import com.facebook.presto.plugin.clp.split.filter.ClpSplitFilterProvider;
import com.facebook.presto.spi.PrestoException;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import static com.facebook.airlift.configuration.ConfigBinder.configBinder;
import static com.facebook.presto.plugin.clp.ClpConfig.MetadataProviderType;
import static com.facebook.presto.plugin.clp.ClpConfig.SplitFilterProviderType;
import static com.facebook.presto.plugin.clp.ClpConfig.SplitProviderType;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_UNSUPPORTED_METADATA_SOURCE;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_UNSUPPORTED_SPLIT_FILTER_SOURCE;
import static com.facebook.presto.plugin.clp.ClpErrorCode.CLP_UNSUPPORTED_SPLIT_SOURCE;

public class ClpModule
        extends AbstractConfigurationAwareModule
{
    @Override
    protected void setup(Binder binder)
    {
        binder.bind(ClpConnector.class).in(Scopes.SINGLETON);
        binder.bind(ClpMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ClpRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ClpSplitManager.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ClpConfig.class);

        ClpConfig config = buildConfigObject(ClpConfig.class);

        if (SplitFilterProviderType.MYSQL == config.getSplitFilterProviderType()) {
            binder.bind(ClpSplitFilterProvider.class).to(ClpMySqlSplitFilterProvider.class).in(Scopes.SINGLETON);
        }
        else {
            throw new PrestoException(CLP_UNSUPPORTED_SPLIT_FILTER_SOURCE, "Unsupported split filter provider type: " + config.getSplitFilterProviderType());
        }

        if (config.getMetadataProviderType() == MetadataProviderType.MYSQL) {
            binder.bind(ClpMetadataProvider.class).to(ClpMySqlMetadataProvider.class).in(Scopes.SINGLETON);
        }
        else {
            throw new PrestoException(CLP_UNSUPPORTED_METADATA_SOURCE, "Unsupported metadata provider type: " + config.getMetadataProviderType());
        }

        if (config.getSplitProviderType() == SplitProviderType.MYSQL) {
            binder.bind(ClpSplitProvider.class).to(ClpMySqlSplitProvider.class).in(Scopes.SINGLETON);
        }
        else {
            throw new PrestoException(CLP_UNSUPPORTED_SPLIT_SOURCE, "Unsupported split provider type: " + config.getSplitProviderType());
        }
    }
}
