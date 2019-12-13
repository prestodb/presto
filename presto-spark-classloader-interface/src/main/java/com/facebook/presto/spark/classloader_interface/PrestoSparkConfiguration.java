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
package com.facebook.presto.spark.classloader_interface;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

public class PrestoSparkConfiguration
{
    private final String configFilePath;
    private final String pluginsDirectoryPath;
    private final String pluginsConfigDirectoryPath;
    private final Map<String, String> extraProperties;

    public PrestoSparkConfiguration(
            String configFilePath,
            String pluginsDirectoryPath,
            String pluginsConfigDirectoryPath,
            Map<String, String> extraProperties)
    {
        this.configFilePath = requireNonNull(configFilePath, "configFilePath is null");
        this.pluginsDirectoryPath = requireNonNull(pluginsDirectoryPath, "pluginsDirectoryPath is null");
        this.pluginsConfigDirectoryPath = requireNonNull(pluginsConfigDirectoryPath, "pluginsConfigDirectoryPath is null");
        this.extraProperties = unmodifiableMap(new HashMap<>(requireNonNull(extraProperties, "extraProperties is null")));
    }

    public String getConfigFilePath()
    {
        return configFilePath;
    }

    public String getPluginsDirectoryPath()
    {
        return pluginsDirectoryPath;
    }

    public String getPluginsConfigDirectoryPath()
    {
        return pluginsConfigDirectoryPath;
    }

    public Map<String, String> getExtraProperties()
    {
        return extraProperties;
    }
}
