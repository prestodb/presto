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
package com.facebook.presto.orc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

/**
 * Convenient holder for enabled features and their configs.
 */
public class OrcFeatures
{
    private final Set<OrcFeature<?>> enabledFeatures;
    private final Map<OrcFeature<?>, Object> featureConfigs;

    public OrcFeatures()
    {
        this.enabledFeatures = ImmutableSet.of();
        this.featureConfigs = ImmutableMap.of();
    }

    public OrcFeatures(Set<OrcFeature<?>> enabledFeatures, Map<OrcFeature<?>, Object> featureConfigs)
    {
        this.enabledFeatures = ImmutableSet.copyOf(requireNonNull(enabledFeatures, "enabledFeatures is null"));
        this.featureConfigs = ImmutableMap.copyOf(requireNonNull(featureConfigs, "featureConfigs is null"));
    }

    public <T> boolean isEnabled(OrcFeature<T> feature)
    {
        requireNonNull(feature, "feature is null");
        return enabledFeatures.contains(feature);
    }

    public <T> Optional<T> getFeatureConfig(OrcFeature<T> feature)
    {
        requireNonNull(feature, "feature is null");
        return Optional.ofNullable((T) featureConfigs.get(feature));
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private final Set<OrcFeature<?>> enabledFeatures = new HashSet<>();
        private final Map<OrcFeature<?>, Object> featureConfigs = new HashMap<>();

        /**
         * Enable a feature without adding a config.
         */
        public <T> Builder enable(OrcFeature<T> feature)
        {
            enabledFeatures.add(feature);
            return this;
        }

        /**
         * Enable a feature without adding a config only if enabled flag is true.
         */
        public <T> Builder enable(OrcFeature<T> feature, boolean enabled)
        {
            if (enabled) {
                enable(feature);
            }
            return this;
        }

        /**
         * Enable feature and add a custom config.
         */
        public <T> Builder enable(OrcFeature<T> feature, T config)
        {
            enabledFeatures.add(feature);
            featureConfigs.put(feature, config);
            return this;
        }

        /**
         * Enable feature and add a custom config if enabled flag is true.
         */
        public <T> Builder enable(OrcFeature<T> feature, T config, boolean enabled)
        {
            if (enabled) {
                enable(feature, config);
            }
            return this;
        }

        public OrcFeatures build()
        {
            return new OrcFeatures(enabledFeatures, featureConfigs);
        }
    }
}
