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
import com.facebook.presto.plugin.clp.metadata.ClpYamlMetadataProvider;
import com.facebook.presto.plugin.clp.split.ClpMySqlSplitProvider;
import com.facebook.presto.plugin.clp.split.ClpPinotSplitProvider;
import com.facebook.presto.plugin.clp.split.ClpSplitProvider;
import com.facebook.presto.plugin.clp.split.ClpUberPinotSplitProvider;
import com.facebook.presto.plugin.clp.split.filter.ClpMySqlSplitFilterProvider;
import com.facebook.presto.plugin.clp.split.filter.ClpPinotSplitFilterProvider;
import com.facebook.presto.plugin.clp.split.filter.ClpSplitFilterProvider;
import com.facebook.presto.plugin.clp.split.filter.ClpUberPinotSplitFilterProvider;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import com.google.inject.Binder;
import com.google.inject.Scopes;

import java.util.Map;

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
    // Provider mappings for cleaner configuration binding
    private static final Map<SplitFilterProviderType, Class<? extends ClpSplitFilterProvider>> SPLIT_FILTER_PROVIDER_MAPPINGS =
            ImmutableMap.<SplitFilterProviderType, Class<? extends ClpSplitFilterProvider>>builder()
                    .put(SplitFilterProviderType.MYSQL, ClpMySqlSplitFilterProvider.class)
                    .put(SplitFilterProviderType.PINOT, ClpPinotSplitFilterProvider.class)
                    .put(SplitFilterProviderType.PINOT_UBER, ClpUberPinotSplitFilterProvider.class)
                    .build();

    private static final Map<MetadataProviderType, Class<? extends ClpMetadataProvider>> METADATA_PROVIDER_MAPPINGS =
            ImmutableMap.<MetadataProviderType, Class<? extends ClpMetadataProvider>>builder()
                    .put(MetadataProviderType.MYSQL, ClpMySqlMetadataProvider.class)
                    .put(MetadataProviderType.YAML, ClpYamlMetadataProvider.class)
                    .build();

    private static final Map<SplitProviderType, Class<? extends ClpSplitProvider>> SPLIT_PROVIDER_MAPPINGS =
            ImmutableMap.<SplitProviderType, Class<? extends ClpSplitProvider>>builder()
                    .put(SplitProviderType.MYSQL, ClpMySqlSplitProvider.class)
                    .put(SplitProviderType.PINOT, ClpPinotSplitProvider.class)
                    .put(SplitProviderType.PINOT_UBER, ClpUberPinotSplitProvider.class)
                    .build();

    @Override
    protected void setup(Binder binder)
    {
        binder.bind(ClpConnector.class).in(Scopes.SINGLETON);
        binder.bind(ClpMetadata.class).in(Scopes.SINGLETON);
        binder.bind(ClpRecordSetProvider.class).in(Scopes.SINGLETON);
        binder.bind(ClpSplitManager.class).in(Scopes.SINGLETON);
        configBinder(binder).bindConfig(ClpConfig.class);

        ClpConfig config = buildConfigObject(ClpConfig.class);

        // Bind split filter provider
        Class<? extends ClpSplitFilterProvider> splitFilterProviderClass =
                SPLIT_FILTER_PROVIDER_MAPPINGS.get(config.getSplitFilterProviderType());
        if (splitFilterProviderClass == null) {
            throw new PrestoException(CLP_UNSUPPORTED_SPLIT_FILTER_SOURCE,
                    "Unsupported split filter provider type: " + config.getSplitFilterProviderType());
        }
        binder.bind(ClpSplitFilterProvider.class).to(splitFilterProviderClass).in(Scopes.SINGLETON);

        // Bind metadata provider
        Class<? extends ClpMetadataProvider> metadataProviderClass =
                METADATA_PROVIDER_MAPPINGS.get(config.getMetadataProviderType());
        if (metadataProviderClass == null) {
            throw new PrestoException(CLP_UNSUPPORTED_METADATA_SOURCE,
                    "Unsupported metadata provider type: " + config.getMetadataProviderType());
        }
        binder.bind(ClpMetadataProvider.class).to(metadataProviderClass).in(Scopes.SINGLETON);

        // Bind split provider
        Class<? extends ClpSplitProvider> splitProviderClass =
                SPLIT_PROVIDER_MAPPINGS.get(config.getSplitProviderType());
        if (splitProviderClass == null) {
            throw new PrestoException(CLP_UNSUPPORTED_SPLIT_SOURCE,
                    "Unsupported split provider type: " + config.getSplitProviderType());
        }
        binder.bind(ClpSplitProvider.class).to(splitProviderClass).in(Scopes.SINGLETON);
    }
}
