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
package com.facebook.presto.execution;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.presto.spi.resourceGroups.ResourceGroupId;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.execution.ClusterOverloadConfig.OVERLOAD_POLICY_CNT_BASED;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertThrows;

public class TestClusterOverloadConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ClusterOverloadConfig.class)
                .setClusterOverloadThrottlingEnabled(false)
                .setAllowedOverloadWorkersPct(0.01)
                .setAllowedOverloadWorkersCnt(0)
                .setOverloadPolicyType(OVERLOAD_POLICY_CNT_BASED)
                .setOverloadCheckCacheTtlInSecs(5)
                .setThrottlingBypassResourceGroups(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("cluster-overload.enable-throttling", "true")
                .put("cluster-overload.allowed-overload-workers-pct", "0.05")
                .put("cluster-overload.allowed-overload-workers-cnt", "5")
                .put("cluster-overload.overload-policy-type", "overload_worker_pct_based_throttling")
                .put("cluster.overload-check-cache-ttl-secs", "10")
                .put("cluster-overload.bypass-resource-groups", "global.admin, global.etl.priority")
                .build();

        ClusterOverloadConfig expected = new ClusterOverloadConfig()
                .setClusterOverloadThrottlingEnabled(true)
                .setAllowedOverloadWorkersPct(0.05)
                .setAllowedOverloadWorkersCnt(5)
                .setOverloadPolicyType("overload_worker_pct_based_throttling")
                .setOverloadCheckCacheTtlInSecs(10)
                .setThrottlingBypassResourceGroups("global.admin, global.etl.priority");

        ConfigAssertions.assertFullMapping(properties, expected);
    }

    @Test
    public void testThrottlingBypassParsing()
    {
        ClusterOverloadConfig config = new ClusterOverloadConfig();
        assertEquals(config.getThrottlingBypassResourceGroups(), ImmutableSet.of());

        config.setThrottlingBypassResourceGroups("global.admin, global.etl, ,global.adhoc");
        assertEquals(
                config.getThrottlingBypassResourceGroups(),
                ImmutableSet.of(
                        new ResourceGroupId(ImmutableList.of("global", "admin")),
                        new ResourceGroupId(ImmutableList.of("global", "etl")),
                        new ResourceGroupId(ImmutableList.of("global", "adhoc"))));

        config.setThrottlingBypassResourceGroups(null);
        assertEquals(config.getThrottlingBypassResourceGroups(), ImmutableSet.of());

        config.setThrottlingBypassResourceGroups("");
        assertEquals(config.getThrottlingBypassResourceGroups(), ImmutableSet.of());
    }

    @Test
    public void testThrottlingBypassMalformedEntryFailsFast()
    {
        // Empty segments are rejected by ResourceGroupId; airlift Bootstrap surfaces this as a
        // configuration error tagged with the property name.
        ClusterOverloadConfig config = new ClusterOverloadConfig();
        assertThrows(IllegalArgumentException.class,
                () -> config.setThrottlingBypassResourceGroups("global..admin"));
    }
}
