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

import com.facebook.presto.features.config.TestFeatureToggleConfiguration;
import com.facebook.presto.features.strategy.BooleanStringStrategy;
import com.facebook.presto.features.strategy.FeatureToggleStrategyFactory;
import com.facebook.presto.spi.features.FeatureConfiguration;
import com.facebook.presto.spi.features.FeatureToggleConfiguration;
import com.facebook.presto.spi.features.FeatureToggleStrategyConfig;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.util.HashMap;
import java.util.Map;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class PrestoFeatureToggleTest
{
    private static final String simpleFeatureId = "simple-feature";
    private static final String simpleFeatureWithStrategyToggleId = "simple-feature-with-strategy-toggle";
    /**
     * map is configuration provider for dynamic configuration parameters
     */
    private final Map<String, FeatureConfiguration> config = new HashMap<>();
    private final Map<String, Object> featureInstanceMap = new HashMap<>();
    private final FeatureToggleStrategyFactory featureToggleStrategyFactory = new FeatureToggleStrategyFactory(ImmutableMap.of("BooleanString", new BooleanStringStrategy()));
    private Map<String, Feature<?>> featureMap;
    private FeatureToggleConfiguration featureToggleConfiguration;

    @BeforeMethod
    public void prepare()
    {
        config.clear();
        featureMap = new HashMap<>();
        FeatureToggleStrategyConfig booleanStringStrategyConfig = new FeatureToggleStrategyConfig("BooleanString", ImmutableMap.of("allow-values", "yes,no"));

        FeatureConfiguration simpleFeatureConfiguration = FeatureConfiguration.builder()
                .featureId(simpleFeatureId)
                .build();
        Feature<String> simpleFeature = new Feature<>(simpleFeatureId, null, simpleFeatureConfiguration);
        featureMap.put(simpleFeatureId, simpleFeature);

        FeatureConfiguration simpleFeatureWithStrategyToggleConfiguration = FeatureConfiguration.builder()
                .featureId(simpleFeatureWithStrategyToggleId)
                .featureToggleStrategyConfig(booleanStringStrategyConfig)
                .build();
        Feature<String> simpleFeatureWithStrategyToggle = new Feature<>(simpleFeatureWithStrategyToggleId, null, simpleFeatureWithStrategyToggleConfiguration);
        featureMap.put(simpleFeatureWithStrategyToggleId, simpleFeatureWithStrategyToggle);

        featureToggleConfiguration = new TestFeatureToggleConfiguration(config);
    }

    @Test
    public void testSimpleEnabledDisableToggle()
    {
        PrestoFeatureToggle prestoFeatureToggle = new PrestoFeatureToggle(featureMap, featureInstanceMap, featureToggleStrategyFactory, featureToggleConfiguration);
        boolean enabled;
        enabled = prestoFeatureToggle.isEnabled(simpleFeatureId);
        assertTrue(enabled);

        // change configuration
        config.put(simpleFeatureId, FeatureConfiguration.builder().featureId(simpleFeatureId).enabled(false).build());
        enabled = prestoFeatureToggle.isEnabled(simpleFeatureId);
        assertFalse(enabled);
    }

    @Test
    public void testSimpleEnabledDisabledToggleWithStrategy()
    {
        PrestoFeatureToggle prestoFeatureToggle = new PrestoFeatureToggle(featureMap, featureInstanceMap, featureToggleStrategyFactory, featureToggleConfiguration);
        boolean enabled;
        // features are enabled by default
        enabled = prestoFeatureToggle.isEnabled(simpleFeatureWithStrategyToggleId);
        assertTrue(enabled);

        // change configuration set enabled to false
        config.put(simpleFeatureWithStrategyToggleId, FeatureConfiguration.builder().featureId(simpleFeatureWithStrategyToggleId).enabled(false).build());

        enabled = prestoFeatureToggle.isEnabled(simpleFeatureWithStrategyToggleId);
        assertFalse(enabled);
    }

    @Test
    public void testStrategyEnabledDisabledToggleWithStrategy()
    {
        PrestoFeatureToggle prestoFeatureToggle = new PrestoFeatureToggle(featureMap, featureInstanceMap, featureToggleStrategyFactory, featureToggleConfiguration);
        boolean enabled;

        // feature with id = simpleFeatureWithStrategyToggleId is configured to use BooleanStringStrategy feature toggle strategy
        enabled = prestoFeatureToggle.isEnabled(simpleFeatureWithStrategyToggleId);
        assertTrue(enabled);

        // initially feature is configured to use BooleanStringStrategy with allowed values "yes,no"
        enabled = prestoFeatureToggle.isEnabled(simpleFeatureWithStrategyToggleId, "yes");
        assertTrue(enabled);

        enabled = prestoFeatureToggle.isEnabled(simpleFeatureWithStrategyToggleId, "no");
        assertFalse(enabled);

        // change configuration sets feature to disabled
        config.put(simpleFeatureWithStrategyToggleId, FeatureConfiguration.builder()
                .featureId(simpleFeatureWithStrategyToggleId)
                .enabled(false)
                .build());
        enabled = prestoFeatureToggle.isEnabled(simpleFeatureWithStrategyToggleId);
        assertFalse(enabled);

        enabled = prestoFeatureToggle.isEnabled(simpleFeatureWithStrategyToggleId, "yes");
        assertFalse(enabled);

        enabled = prestoFeatureToggle.isEnabled(simpleFeatureWithStrategyToggleId, "no");
        assertFalse(enabled);

        // change configuration
        FeatureToggleStrategyConfig booleanStringStrategyConfigTrueFalse = new FeatureToggleStrategyConfig("BooleanString", ImmutableMap.of("allow-values", "true,false"));
        config.put(simpleFeatureWithStrategyToggleId, FeatureConfiguration.builder()
                .featureId(simpleFeatureWithStrategyToggleId)
                .enabled(true)
                .featureToggleStrategyConfig(booleanStringStrategyConfigTrueFalse).build());
        enabled = prestoFeatureToggle.isEnabled(simpleFeatureWithStrategyToggleId);
        assertTrue(enabled);

        enabled = prestoFeatureToggle.isEnabled(simpleFeatureWithStrategyToggleId, "yes");
        assertFalse(enabled);

        enabled = prestoFeatureToggle.isEnabled(simpleFeatureWithStrategyToggleId, "no");
        assertFalse(enabled);

        enabled = prestoFeatureToggle.isEnabled(simpleFeatureWithStrategyToggleId, "true");
        assertTrue(enabled);

        enabled = prestoFeatureToggle.isEnabled(simpleFeatureWithStrategyToggleId, "false");
        assertFalse(enabled);

        // change configuration
        config.put(simpleFeatureWithStrategyToggleId, FeatureConfiguration.builder().featureId(simpleFeatureWithStrategyToggleId).enabled(false).build());
        enabled = prestoFeatureToggle.isEnabled(simpleFeatureWithStrategyToggleId);
        assertFalse(enabled);
    }
}
