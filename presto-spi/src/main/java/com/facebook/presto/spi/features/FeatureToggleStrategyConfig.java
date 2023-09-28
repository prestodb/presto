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
package com.facebook.presto.spi.features;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

public class FeatureToggleStrategyConfig
{
    private static final String ACTIVE = "active";

    private final Map<String, String> configurationMap;
    private final String strategyName;

    public FeatureToggleStrategyConfig(
            String strategyName,
            Map<String, String> configurationMap)
    {
        this.strategyName = strategyName;
        this.configurationMap = configurationMap;
    }

    public boolean active()
    {
        if (configurationMap.containsKey(ACTIVE)) {
            return Boolean.parseBoolean(configurationMap.get(ACTIVE));
        }
        return true;
    }

    public String getToggleStrategyName()
    {
        return strategyName;
    }

    public Map<String, String> getConfigurationMap()
    {
        List<String> properties = Arrays.asList(ACTIVE, "");
        return configurationMap.entrySet().stream()
                .filter(e -> !properties.contains(e.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    public Optional<String> get(String key)
    {
        return Optional.ofNullable(configurationMap.get(key));
    }
}
