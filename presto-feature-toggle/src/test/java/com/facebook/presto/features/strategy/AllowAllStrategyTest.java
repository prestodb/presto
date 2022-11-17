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
import org.testng.annotations.Test;

import static org.testng.Assert.assertTrue;

public class AllowAllStrategyTest
{
    @Test
    public void test()
    {
        boolean enabled;
        AllowAllStrategy allowAllStrategy = new AllowAllStrategy();
        enabled = allowAllStrategy.check(FeatureConfiguration.builder().featureId("test feature").build());
        assertTrue(enabled);
        enabled = allowAllStrategy.check(FeatureConfiguration.builder().featureId("test feature").build(), new Object());
        assertTrue(enabled);
    }
}
