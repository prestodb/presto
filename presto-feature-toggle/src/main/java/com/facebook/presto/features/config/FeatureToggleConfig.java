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
package com.facebook.presto.features.config;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import io.airlift.units.Duration;
import io.airlift.units.MinDuration;

public class FeatureToggleConfig
{
    public static final String FEATURES_CONFIG_SOURCE_TYPE = "features.config-source-type";
    public static final String FEATURES_REFRESH_PERIOD = "features.refresh-period";
    public static final String FEATURE_CONFIGURATION_SOURCES_DIRECTORY = "features.configuration-directory";

    private String configSourceType;
    private String configDirectory;
    private Duration refreshPeriod;

    @MinDuration("1ms")
    public Duration getRefreshPeriod()
    {
        return refreshPeriod;
    }

    @Config(FEATURES_REFRESH_PERIOD)
    @ConfigDescription("Configuration refresh period")
    public FeatureToggleConfig setRefreshPeriod(Duration refreshPeriod)
    {
        this.refreshPeriod = refreshPeriod;
        return this;
    }

    public String getConfigSourceType()
    {
        return configSourceType;
    }

    @Config(FEATURES_CONFIG_SOURCE_TYPE)
    @ConfigDescription("Feature configuration source type. Different configuration sources can be added using plugin mechanism.")
    public FeatureToggleConfig setConfigSourceType(String configSourceType)
    {
        this.configSourceType = configSourceType;
        return this;
    }

    public String getConfigDirectory()
    {
        return configDirectory;
    }

    @Config(FEATURE_CONFIGURATION_SOURCES_DIRECTORY)
    @ConfigDescription("Configuration sources directory. Properties files, named `configSourceType`.properties, for each configuration source type.")
    public FeatureToggleConfig setConfigDirectory(String configDirectory)
    {
        this.configDirectory = configDirectory;
        return this;
    }
}
