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

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.presto.features.config.FeatureToggleConfig.FEATURES_CONFIG_SOURCE;
import static com.facebook.presto.features.config.FeatureToggleConfig.FEATURES_CONFIG_SOURCE_TYPE;
import static com.facebook.presto.features.config.FeatureToggleConfig.FEATURES_CONFIG_TYPE;
import static com.facebook.presto.features.config.FeatureToggleConfig.FEATURES_REFRESH_PERIOD;
import static com.facebook.presto.features.config.FeatureToggleConfig.FEATURE_CONFIGURATION_SOURCES_DIRECTORY;

public class FeatureToggleConfigTest
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(ConfigAssertions.recordDefaults(FeatureToggleConfig.class)
                .setConfigSource(null)
                .setConfigSourceType(null)
                .setConfigType(null)
                .setRefreshPeriod(null)
                .setConfigDirectory(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put(FEATURES_CONFIG_SOURCE, "/test.json")
                .put(FEATURES_REFRESH_PERIOD, "1s")
                .put(FEATURES_CONFIG_TYPE, "json")
                .put(FEATURES_CONFIG_SOURCE_TYPE, "file")
                .put(FEATURE_CONFIGURATION_SOURCES_DIRECTORY, "etc/feature-toggle/")
                .build();

        FeatureToggleConfig expected = new FeatureToggleConfig()
                .setConfigSource("/test.json")
                .setRefreshPeriod(new Duration(1, TimeUnit.SECONDS))
                .setConfigSourceType("file")
                .setConfigType("json")
                .setConfigDirectory("etc/feature-toggle/");

        assertFullMapping(properties, expected);
    }
}
