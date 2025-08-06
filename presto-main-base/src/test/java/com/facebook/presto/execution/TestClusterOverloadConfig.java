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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.presto.execution.ClusterOverloadConfig.OVERLOAD_POLICY_CNT_BASED;
import static com.facebook.presto.execution.ClusterOverloadConfig.OVERLOAD_POLICY_PCT_BASED;

public class TestClusterOverloadConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ClusterOverloadConfig.class)
                .setClusterOverloadThrottlingEnabled(false)
                .setAllowedOverloadWorkersPct(0.01)
                .setAllowedOverloadWorkersCnt(1.0)
                .setOverloadPolicyType(OVERLOAD_POLICY_CNT_BASED));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("cluster-overload.enable-throttling", "true")
                .put("cluster-overload.allowed-overload-workers-pct", "0.05")
                .put("cluster-overload.allowed-overload-workers-cnt", "3.0")
                .put("cluster-overload.overload-policy-type", OVERLOAD_POLICY_PCT_BASED)
                .build();

        ClusterOverloadConfig expected = new ClusterOverloadConfig()
                .setClusterOverloadThrottlingEnabled(true)
                .setAllowedOverloadWorkersPct(0.05)
                .setAllowedOverloadWorkersCnt(3.0)
                .setOverloadPolicyType(OVERLOAD_POLICY_PCT_BASED);

        assertFullMapping(properties, expected);
    }
}
