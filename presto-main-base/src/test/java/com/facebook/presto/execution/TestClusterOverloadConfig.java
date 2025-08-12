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
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.execution.ClusterOverloadConfig.OVERLOAD_POLICY_CNT_BASED;

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
                .setOverloadCheckCacheTtlInSecs(5));
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
                .build();

        ClusterOverloadConfig expected = new ClusterOverloadConfig()
                .setClusterOverloadThrottlingEnabled(true)
                .setAllowedOverloadWorkersPct(0.05)
                .setAllowedOverloadWorkersCnt(5)
                .setOverloadPolicyType("overload_worker_pct_based_throttling")
                .setOverloadCheckCacheTtlInSecs(10);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
