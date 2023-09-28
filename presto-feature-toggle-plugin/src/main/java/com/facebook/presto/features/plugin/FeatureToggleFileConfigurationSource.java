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
package com.facebook.presto.features.plugin;

import com.facebook.presto.features.config.DefaultFeatureToggleConfiguration;
import com.facebook.presto.spi.features.ConfigurationSource;
import com.facebook.presto.spi.features.ConfigurationSourceFactory;
import com.facebook.presto.spi.features.FeatureToggleConfiguration;

import java.util.Map;

import static com.facebook.presto.features.config.ConfigurationParser.parseConfiguration;
import static com.google.common.base.Preconditions.checkState;

/**
 * The "file" Feature Toggle configuration source.
 * Configuration source loads Feature toggle configuration form file.
 * File can be in properties or json format.
 * Configuration Source takes two parameter type (properties or json) and location (physical location of the file)
 */
public class FeatureToggleFileConfigurationSource
        implements ConfigurationSource
{
    public static final String NAME = "file";
    public static final String FEATURES_CONFIG_SOURCE = "features.config-source";
    public static final String FEATURES_CONFIG_SOURCE_TYPE = "features.config-type";

    private final String location;
    private final String type;

    public FeatureToggleFileConfigurationSource(String location, String type)
    {
        this.location = location;
        this.type = type;
    }

    @Override
    public FeatureToggleConfiguration getConfiguration()
    {
        return new DefaultFeatureToggleConfiguration(parseConfiguration(location, type));
    }

    public static class Factory
            implements ConfigurationSourceFactory
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public ConfigurationSource create(Map<String, String> config)
        {
            String location = config.get(FEATURES_CONFIG_SOURCE);
            checkState(location != null, "Configuration source path configuration must contain the '%s' property", FEATURES_CONFIG_SOURCE);
            String type = config.get(FEATURES_CONFIG_SOURCE_TYPE);
            checkState(type != null, "Configuration type configuration must contain the '%s' property", FEATURES_CONFIG_SOURCE_TYPE);
            return new FeatureToggleFileConfigurationSource(location, type);
        }
    }
}
