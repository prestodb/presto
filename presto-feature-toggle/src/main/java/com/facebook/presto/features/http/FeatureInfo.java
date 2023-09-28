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
import com.facebook.presto.features.binder.PrestoFeatureToggle;
import com.facebook.presto.spi.features.FeatureConfiguration;
import com.facebook.presto.spi.features.FeatureToggleConfiguration;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@ThriftStruct
public class FeatureInfo
{
    private final String featureId;
    private final boolean enabled;
    private final String featureClass;
    private final List<String> featureInstances;
    private final String currentInstance;
    private final String defaultInstance;
    private final FeatureStrategyInfo strategy;

    public FeatureInfo(
            String featureId,
            boolean enabled,
            String featureClass,
            List<String> featureInstances,
            String currentInstance,
            String defaultInstance,
            FeatureStrategyInfo strategy)
    {
        this.featureId = featureId;
        this.enabled = enabled;
        this.featureClass = featureClass;
        this.featureInstances = featureInstances;
        this.currentInstance = currentInstance;
        this.defaultInstance = defaultInstance;
        this.strategy = strategy;
    }

    static List<FeatureInfo> featureInfos(PrestoFeatureToggle prestoFeatureToggle)
    {
        Map<String, Feature<?>> featureMap = prestoFeatureToggle.getFeatureMap();
        return featureMap.values().stream()
                .map(feature -> getActiveFeatureInfo(feature, prestoFeatureToggle))
                .collect(Collectors.toList());
    }

    static FeatureInfo getActiveFeatureInfo(Feature<?> feature, PrestoFeatureToggle prestoFeatureToggle)
    {
        String featureId = feature.getFeatureId();
        Object featureCurrentInstance = feature.getCurrentInstance(featureId);
        String currentInstanceClass = null;
        if (featureCurrentInstance != null) {
            currentInstanceClass = featureCurrentInstance.getClass().getName();
        }

        FeatureToggleConfiguration featureToggleConfiguration = prestoFeatureToggle.getFeatureToggleConfiguration();
        FeatureStrategyInfo featureStrategyInfo = FeatureStrategyInfo.activeFeatureStrategyInfo(feature, featureToggleConfiguration);

        return new FeatureInfo(
                featureId,
                prestoFeatureToggle.isEnabled(featureId),
                feature.getConfiguration().getFeatureClass(),
                feature.getConfiguration().getFeatureInstances(),
                currentInstanceClass,
                feature.getConfiguration().getDefaultInstance(),
                featureStrategyInfo);
    }

    static FeatureInfo getOverrideFeatureInfo(Feature<?> feature, PrestoFeatureToggle prestoFeatureToggle)
    {
        FeatureToggleConfiguration featureToggleConfiguration = prestoFeatureToggle.getFeatureToggleConfiguration();
        FeatureConfiguration featureConfigurationOverride = featureToggleConfiguration.getFeatureConfiguration(feature.getFeatureId());
        String currentInstanceClass = featureConfigurationOverride.getCurrentInstance();
        FeatureStrategyInfo overrideFeatureStrategyInfo = FeatureStrategyInfo.overrideFeatureStrategyInfo(feature, featureToggleConfiguration);
        return new FeatureInfo(
                feature.getFeatureId(),
                featureConfigurationOverride.isEnabled(),
                null,
                null,
                currentInstanceClass,
                null,
                overrideFeatureStrategyInfo);
    }

    static FeatureInfo getInitialFeatureInfo(Feature<?> feature)
    {
        FeatureStrategyInfo featureStrategyInfo = FeatureStrategyInfo.initialFeatureStrategyInfo(feature);
        return new FeatureInfo(
                feature.getFeatureId(),
                feature.isEnabled(),
                feature.getConfiguration().getFeatureClass(),
                feature.getConfiguration().getFeatureInstances(),
                feature.getConfiguration().getDefaultInstance(),
                feature.getConfiguration().getDefaultInstance(),
                featureStrategyInfo);
    }

    @JsonProperty
    @ThriftField(1)
    public String getFeatureId()
    {
        return featureId;
    }

    @JsonProperty
    @ThriftField(1)
    public boolean isEnabled()
    {
        return enabled;
    }

    @JsonProperty
    @ThriftField(1)
    public String getFeatureClass()
    {
        return featureClass;
    }

    @JsonProperty
    @ThriftField(1)
    public List<String> getFeatureInstances()
    {
        return featureInstances;
    }

    @JsonProperty
    @ThriftField(1)
    public String getCurrentInstance()
    {
        return currentInstance;
    }

    @JsonProperty
    @ThriftField(1)
    public String getDefaultInstance()
    {
        return defaultInstance;
    }

    @JsonProperty
    @ThriftField(1)
    public FeatureStrategyInfo getStrategy()
    {
        return strategy;
    }
}
