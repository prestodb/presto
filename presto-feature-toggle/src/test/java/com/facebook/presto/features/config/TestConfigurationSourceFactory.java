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

import com.facebook.presto.spi.features.ConfigurationSource;
import com.facebook.presto.spi.features.ConfigurationSourceFactory;

import java.util.Map;

public class TestConfigurationSourceFactory
        implements ConfigurationSourceFactory
{
    private final ConfigurationSource configurationSource;

    public TestConfigurationSourceFactory(ConfigurationSource configurationSource)
    {
        this.configurationSource = configurationSource;
    }

    @Override
    public String getName()
    {
        return TestConfigurationSource.NAME;
    }

    @Override
    public ConfigurationSource create(Map<String, String> config)
    {
        return configurationSource;
    }
}
