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
package com.facebook.presto.features.binder;

import com.facebook.presto.features.annotations.FeatureToggle;
import com.facebook.presto.features.strategy.FeatureToggleStrategy;
import com.facebook.presto.features.strategy.FeatureToggleStrategyFactory;
import com.facebook.presto.spi.features.FeatureConfiguration;
import com.facebook.presto.spi.features.FeatureToggleConfiguration;
import com.facebook.presto.spi.features.FeatureToggleStrategyConfig;
import com.google.inject.Inject;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class PrestoFeatureToggle
{
    private final Map<String, Feature<?>> featureMap;
    private final Map<String, Object> featureInstanceMap;
    private final FeatureToggleStrategyFactory featureToggleStrategyFactory;
    private final FeatureToggleConfiguration featureToggleConfiguration;

    @Inject
    public PrestoFeatureToggle(
            @FeatureToggle("feature-map") Map<String, Feature<?>> featureMap,
            @FeatureToggle("feature-instance-map") Map<String, Object> featureInstanceMap,
            FeatureToggleStrategyFactory featureToggleStrategyFactory,
            FeatureToggleConfiguration featureToggleConfiguration)
    {
        this.featureMap = requireNonNull(featureMap);
        this.featureInstanceMap = requireNonNull(featureInstanceMap);
        this.featureToggleStrategyFactory = requireNonNull(featureToggleStrategyFactory);
        this.featureToggleConfiguration = requireNonNull(featureToggleConfiguration);
        this.featureMap.values().forEach(f -> f.setContext(this));
    }

    public boolean isEnabled(String featureId)
    {
        Feature<?> feature = featureMap.get(featureId);
        FeatureConfiguration featureConfiguration = feature.getConfiguration();
        AtomicBoolean enabled = new AtomicBoolean(true);
        FeatureConfiguration configuration = featureToggleConfiguration.getFeatureConfiguration(featureId);
        if (configuration != null) {
            if (!configuration.isEnabled()) {
                return false;
            }
            enabled.set(configuration.isEnabled());
            if (configuration.getFeatureToggleStrategyConfig().isPresent()) {
                String toggleStrategyClass = configuration.getFeatureToggleStrategyConfig().get().getToggleStrategyName();
                if (toggleStrategyClass != null) {
                    FeatureToggleStrategy strategy = featureToggleStrategyFactory.get(toggleStrategyClass);
                    if (strategy != null) {
                        return enabled.get() && strategy.check(configuration);
                    }
                    else {
                        return enabled.get();
                    }
                }
            }
        }
        else if (featureConfiguration.getFeatureToggleStrategyConfig().isPresent()) {
            FeatureToggleStrategyConfig featureToggleStrategyConfig = featureConfiguration.getFeatureToggleStrategyConfig().get();
            String toggleStrategyClass = featureToggleStrategyConfig.getToggleStrategyName();
            if (toggleStrategyClass != null) {
                FeatureToggleStrategy strategy = featureToggleStrategyFactory.get(toggleStrategyClass);
                if (strategy != null) {
                    return enabled.get() && strategy.check(featureConfiguration);
                }
                else {
                    return enabled.get();
                }
            }
        }
        return enabled.get();
    }

    public boolean isEnabled(String featureId, Object object)
    {
        Feature<?> feature = featureMap.get(featureId);
        FeatureConfiguration featureConfiguration = feature.getConfiguration();
        AtomicBoolean enabled = new AtomicBoolean(feature.isEnabled());
        FeatureConfiguration configuration = featureToggleConfiguration.getFeatureConfiguration(featureId);
        if (configuration != null) {
            enabled.set(configuration.isEnabled());
            if (configuration.getFeatureToggleStrategyConfig().isPresent()) {
                String toggleStrategyClass = configuration.getFeatureToggleStrategyConfig().get().getToggleStrategyName();
                if (toggleStrategyClass != null) {
                    FeatureToggleStrategy strategy = featureToggleStrategyFactory.get(toggleStrategyClass);
                    if (strategy != null) {
                        return enabled.get() && strategy.check(configuration, object);
                    }
                    else {
                        return enabled.get();
                    }
                }
            }
        }
        else if (featureConfiguration.getFeatureToggleStrategyConfig().isPresent()) {
            FeatureToggleStrategyConfig featureToggleStrategyConfig = featureConfiguration.getFeatureToggleStrategyConfig().get();
            String toggleStrategyClass = featureToggleStrategyConfig.getToggleStrategyName();
            if (toggleStrategyClass != null) {
                FeatureToggleStrategy strategy = featureToggleStrategyFactory.get(toggleStrategyClass);
                if (strategy != null) {
                    return enabled.get() && strategy.check(featureConfiguration, object);
                }
                else {
                    return enabled.get();
                }
            }
        }
        return enabled.get();
    }

    public Object getCurrentInstance(String featureId)
    {
        if (featureToggleConfiguration.getFeatureConfiguration(featureId) != null) {
            String currentInstance = featureToggleConfiguration.getCurrentInstance(featureId);
            if (currentInstance != null) {
                return featureInstanceMap.get(currentInstance);
            }
        }
        Feature<?> feature = featureMap.get(featureId);
        String defaultInstance = feature.getConfiguration().getDefaultInstance();
        if (defaultInstance != null) {
            return featureInstanceMap.get(defaultInstance);
        }
        else if (feature.getConfiguration().getFeatureInstances() != null && !feature.getConfiguration().getFeatureInstances().isEmpty()) {
            return featureInstanceMap.get(feature.getConfiguration().getFeatureInstances().get(0));
        }
        else {
            return null;
        }
    }

    public Map<String, Feature<?>> getFeatureMap()
    {
        return featureMap;
    }

    public FeatureToggleConfiguration getFeatureToggleConfiguration()
    {
        return featureToggleConfiguration;
    }
}
