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
import com.facebook.presto.spi.features.FeatureConfiguration;
import com.facebook.presto.spi.features.FeatureToggleConfiguration;
import com.google.inject.Inject;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class PrestoFeatureToggle
{
    private final Map<String, Feature<?>> featureMap;
    private final Map<String, Object> featureInstanceMap;
    private final FeatureToggleConfiguration featureToggleConfiguration;

    @Inject
    public PrestoFeatureToggle(
            @FeatureToggle("feature-map") Map<String, Feature<?>> featureMap,
            @FeatureToggle("feature-instance-map") Map<String, Object> featureInstanceMap,
            FeatureToggleConfiguration featureToggleConfiguration)
    {
        this.featureMap = requireNonNull(featureMap);
        this.featureInstanceMap = requireNonNull(featureInstanceMap);
        this.featureToggleConfiguration = requireNonNull(featureToggleConfiguration);
        this.featureMap.values().forEach(f -> f.setContext(this));
    }

    public boolean isEnabled(String featureId)
    {
        AtomicBoolean enabled = new AtomicBoolean(true);
        checkEnabledFromStaticConfiguration(featureId, enabled);
        checkEnabledFromDynamicConfiguration(featureId, enabled);
        return enabled.get();
    }

    private void checkEnabledFromDynamicConfiguration(String featureId, AtomicBoolean enabled)
    {
        FeatureConfiguration dynamicFeatureConfiguration = featureToggleConfiguration.getFeatureConfiguration(featureId);
        if (dynamicFeatureConfiguration != null) {
            enabled.set(dynamicFeatureConfiguration.isEnabled());
        }
    }

    private void checkEnabledFromStaticConfiguration(String featureId, AtomicBoolean enabled)
    {
        FeatureConfiguration staticFeatureConfiguration = featureMap.get(featureId).getConfiguration();
        enabled.set(staticFeatureConfiguration.isEnabled());
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
