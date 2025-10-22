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
package com.facebook.presto.builtin.tools;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestNativeSidecarRegistryToolConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(NativeSidecarRegistryToolConfig.class)
                .setNativeSidecarRegistryToolNumRetries(8)
                .setNativeSidecarRegistryToolRetryDelayMs(60_000L));
    }

    @Test
    public void testExplicitPropertyMappings()
            throws Exception
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("native-sidecar-registry-tool.num-retries", "15")
                .put("native-sidecar-registry-tool.retry-delay-ms", "11115")
                .build();

        NativeSidecarRegistryToolConfig expected = new NativeSidecarRegistryToolConfig()
                .setNativeSidecarRegistryToolNumRetries(15)
                .setNativeSidecarRegistryToolRetryDelayMs(11_115L);

        assertFullMapping(properties, expected);
    }
}
