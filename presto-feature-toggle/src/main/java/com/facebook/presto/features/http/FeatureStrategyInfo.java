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
package com.facebook.presto.features.http;

import com.facebook.drift.annotations.ThriftField;
import com.facebook.drift.annotations.ThriftStruct;
import com.facebook.presto.features.binder.Feature;
import com.facebook.presto.spi.features.FeatureToggleConfiguration;
import com.facebook.presto.spi.features.FeatureToggleStrategyConfig;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

@ThriftStruct
public class FeatureStrategyInfo
{
    private static final String ACTIVE = "active";

    private final boolean active;
    private final String strategyName;
    private final Map<String, String> config;

    public FeatureStrategyInfo(boolean active, String strategyName, Map<String, String> config)
    {
        this.active = active;
        this.strategyName = strategyName;
        this.config = config;
    }

    static FeatureStrategyInfo activeFeatureStrategyInfo(Feature<?> feature, FeatureToggleConfiguration configuration)
    {
        Optional<FeatureToggleStrategyConfig> strategyConfigOptional = feature.getConfiguration().getFeatureToggleStrategyConfig();
        if (strategyConfigOptional.isPresent()) {
            FeatureToggleStrategyConfig strategyConfig = strategyConfigOptional.get();
            boolean active = strategyConfig.active();
            String toggleStrategyName = strategyConfig.getToggleStrategyName();
            Map<String, String> configMap = new HashMap<>(strategyConfig.getConfigurationMap());
            if (configuration != null) {
                Optional<FeatureToggleStrategyConfig> featureToggleStrategyConfigOptional = configuration.getFeatureConfiguration(feature.getFeatureId()).getFeatureToggleStrategyConfig();
                if (featureToggleStrategyConfigOptional.isPresent()) {
                    FeatureToggleStrategyConfig featureToggleStrategyConfig = featureToggleStrategyConfigOptional.get();
                    configMap.putAll(featureToggleStrategyConfig.getConfigurationMap());
                    if (configMap.containsKey(ACTIVE)) {
                        active = Boolean.parseBoolean(configMap.get(ACTIVE));
                    }
                    toggleStrategyName = featureToggleStrategyConfig.getToggleStrategyName();
                }
            }
            return new FeatureStrategyInfo(active, toggleStrategyName, configMap);
        }
        else {
            return null;
        }
    }

    static FeatureStrategyInfo overrideFeatureStrategyInfo(Feature<?> feature, FeatureToggleConfiguration configuration)
    {
        Optional<FeatureToggleStrategyConfig> strategyConfigOptional = feature.getConfiguration().getFeatureToggleStrategyConfig();
        if (strategyConfigOptional.isPresent()) {
            FeatureToggleStrategyConfig strategyConfig = strategyConfigOptional.get();
            Map<String, String> configMap = new HashMap<>(strategyConfig.getConfigurationMap());
            if (configuration != null) {
                boolean active = false;
                String toggleStrategyName;
                Optional<FeatureToggleStrategyConfig> featureToggleStrategyConfigOptional = configuration.getFeatureConfiguration(feature.getFeatureId()).getFeatureToggleStrategyConfig();
                if (featureToggleStrategyConfigOptional.isPresent()) {
                    FeatureToggleStrategyConfig featureToggleStrategyConfig = featureToggleStrategyConfigOptional.get();
                    configMap.putAll(featureToggleStrategyConfig.getConfigurationMap());
                    if (configMap.containsKey(ACTIVE)) {
                        active = Boolean.parseBoolean(configMap.get(ACTIVE));
                    }
                    toggleStrategyName = featureToggleStrategyConfig.getToggleStrategyName();
                    return new FeatureStrategyInfo(active, toggleStrategyName, configMap);
                }
            }
        }
        return null;
    }

    static FeatureStrategyInfo initialFeatureStrategyInfo(Feature<?> feature)
    {
        Optional<FeatureToggleStrategyConfig> strategyConfigOptional = feature.getConfiguration().getFeatureToggleStrategyConfig();
        if (strategyConfigOptional.isPresent()) {
            FeatureToggleStrategyConfig strategyConfig = strategyConfigOptional.get();
            boolean active = strategyConfig.active();
            String toggleStrategyName = strategyConfig.getToggleStrategyName();
            Map<String, String> configMap = new HashMap<>(strategyConfig.getConfigurationMap());
            return new FeatureStrategyInfo(active, toggleStrategyName, configMap);
        }
        else {
            return null;
        }
    }

    @JsonProperty
    @ThriftField(1)
    public boolean isActive()
    {
        return active;
    }

    @JsonProperty
    @ThriftField(2)
    public String getStrategyName()
    {
        return strategyName;
    }

    @JsonProperty
    @ThriftField(3)
    public Map<String, String> getConfig()
    {
        return config;
    }
}
