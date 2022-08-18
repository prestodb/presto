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
package com.facebook.presto.hive;

import com.google.common.collect.ImmutableSet;
import org.apache.hadoop.conf.Configuration;

import javax.inject.Inject;

import java.net.URI;
import java.util.Set;

import static com.facebook.presto.hive.util.ConfigurationUtils.copy;
import static com.facebook.presto.hive.util.ConfigurationUtils.getInitialConfiguration;
import static java.util.Objects.requireNonNull;

public class HiveHdfsConfiguration
        implements HdfsConfiguration
{
    private static final Configuration INITIAL_CONFIGURATION = getInitialConfiguration();

    @SuppressWarnings("ThreadLocalNotStaticFinal")
    private final ThreadLocal<Configuration> hadoopConfiguration = new ThreadLocal<Configuration>()
    {
        @Override
        protected Configuration initialValue()
        {
            Configuration configuration = new Configuration(false);
            copy(INITIAL_CONFIGURATION, configuration);
            initializer.updateConfiguration(configuration);
            return configuration;
        }
    };

    private final HdfsConfigurationInitializer initializer;
    private final Set<DynamicConfigurationProvider> dynamicProviders;

    // This is set to TRUE if the configuration providers are empty or they do NOT dependent on the URI
    private final boolean isConfigReusable;
    private final boolean isCopyOnFirstWriteConfigurationEnabled;
    private Configuration uriAgnosticConfiguration;

    @Inject
    public HiveHdfsConfiguration(HdfsConfigurationInitializer initializer, Set<DynamicConfigurationProvider> dynamicProviders, HiveClientConfig hiveClientConfig)
    {
        this.initializer = requireNonNull(initializer, "initializer is null");
        this.dynamicProviders = ImmutableSet.copyOf(requireNonNull(dynamicProviders, "dynamicProviders is null"));
        boolean isUriIndependentConfig = true;
        for (DynamicConfigurationProvider provider : dynamicProviders) {
            isUriIndependentConfig = isUriIndependentConfig && provider.isUriIndependentConfigurationProvider();
        }
        this.isConfigReusable = isUriIndependentConfig;
        this.isCopyOnFirstWriteConfigurationEnabled = hiveClientConfig.isCopyOnFirstWriteConfigurationEnabled();
    }

    @Override
    public Configuration getConfiguration(HdfsContext context, URI uri)
    {
        if (dynamicProviders.isEmpty()) {
            // use the same configuration for everything
            return hadoopConfiguration.get();
        }

        if (isCopyOnFirstWriteConfigurationEnabled && isConfigReusable && uriAgnosticConfiguration != null) {
            // use the same configuration for everything
            return new CopyOnFirstWriteConfiguration(uriAgnosticConfiguration);
        }

        Configuration config = new Configuration(hadoopConfiguration.get());
        for (DynamicConfigurationProvider provider : dynamicProviders) {
            provider.updateConfiguration(config, context, uri);
        }

        if (isCopyOnFirstWriteConfigurationEnabled && isConfigReusable && uriAgnosticConfiguration == null) {
            uriAgnosticConfiguration = config;
            // return a CopyOnFirstWrite configuration so that we make a copy before modifying it down the lane
            return new CopyOnFirstWriteConfiguration(uriAgnosticConfiguration);
        }

        return config;
    }
}
