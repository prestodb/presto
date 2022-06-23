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
package com.facebook.presto.execution.scheduler.nodeselection;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.presto.execution.scheduler.nodeSelection.SimpleTtlNodeSelectorConfig;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.Duration;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestSimpleTtlNodeSelectorConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(SimpleTtlNodeSelectorConfig.class)
                .setUseDefaultExecutionTimeEstimateAsFallback(false)
                .setDefaultExecutionTimeEstimate(new Duration(30, TimeUnit.MINUTES)));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("simple-ttl-node-selector.use-default-execution-time-estimate-as-fallback", "true")
                .put("simple-ttl-node-selector.default-execution-time-estimate", "1h")
                .build();

        SimpleTtlNodeSelectorConfig expected = new SimpleTtlNodeSelectorConfig()
                .setUseDefaultExecutionTimeEstimateAsFallback(true)
                .setDefaultExecutionTimeEstimate(new Duration(1, TimeUnit.HOURS));

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
