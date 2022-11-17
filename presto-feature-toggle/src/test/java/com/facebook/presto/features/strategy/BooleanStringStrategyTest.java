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
package com.facebook.presto.features.strategy;

import com.facebook.presto.spi.features.FeatureConfiguration;
import com.facebook.presto.spi.features.FeatureToggleStrategyConfig;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class BooleanStringStrategyTest
{
    @Test
    public void testYesNo()
    {
        boolean enabled;
        BooleanStringStrategy booleanStringStrategy = new BooleanStringStrategy();
        FeatureToggleStrategyConfig featureToggleStrategyConfig = new FeatureToggleStrategyConfig("BooleanStringStrategy", ImmutableMap.of("allow-values", "yes,no"));
        FeatureConfiguration featureConfiguration = FeatureConfiguration.builder().featureId("yes,no feature").featureToggleStrategyConfig(featureToggleStrategyConfig).build();
        enabled = booleanStringStrategy.check(featureConfiguration);
        assertTrue(enabled);
        enabled = booleanStringStrategy.check(featureConfiguration, "yes");
        assertTrue(enabled);
        enabled = booleanStringStrategy.check(featureConfiguration, "no");
        assertFalse(enabled);
        enabled = booleanStringStrategy.check(featureConfiguration, "not sure");
        assertFalse(enabled);
        enabled = booleanStringStrategy.check(featureConfiguration, null);
        assertFalse(enabled);
    }

    @Test
    public void testNotActive()
    {
        boolean enabled;
        BooleanStringStrategy booleanStringStrategy = new BooleanStringStrategy();
        FeatureToggleStrategyConfig featureToggleStrategyConfig = new FeatureToggleStrategyConfig("BooleanStringStrategy", ImmutableMap.of("allow-values", "yes,no", "active", "false"));
        FeatureConfiguration featureConfiguration = FeatureConfiguration.builder().featureId("yes,no feature").featureToggleStrategyConfig(featureToggleStrategyConfig).build();
        enabled = booleanStringStrategy.check(featureConfiguration);
        assertTrue(enabled);
        enabled = booleanStringStrategy.check(featureConfiguration, "yes");
        assertTrue(enabled);
        enabled = booleanStringStrategy.check(featureConfiguration, "no");
        assertTrue(enabled);
        enabled = booleanStringStrategy.check(featureConfiguration, "not sure");
        assertTrue(enabled);
        enabled = booleanStringStrategy.check(featureConfiguration, null);
        assertTrue(enabled);
    }

    @Test
    public void testNoConfiguration()
    {
        boolean enabled;
        BooleanStringStrategy booleanStringStrategy = new BooleanStringStrategy();
        FeatureConfiguration featureConfiguration = FeatureConfiguration.builder().featureId("yes,no feature").build();
        enabled = booleanStringStrategy.check(featureConfiguration);
        assertTrue(enabled);
        enabled = booleanStringStrategy.check(featureConfiguration, "yes");
        assertTrue(enabled);
        enabled = booleanStringStrategy.check(featureConfiguration, "no");
        assertTrue(enabled);
        enabled = booleanStringStrategy.check(featureConfiguration, "not sure");
        assertTrue(enabled);
        enabled = booleanStringStrategy.check(featureConfiguration, null);
        assertTrue(enabled);
    }
}
