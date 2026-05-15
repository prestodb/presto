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
package com.facebook.presto.spark.execution.nativeprocess;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.facebook.airlift.units.Duration;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.concurrent.TimeUnit;

public class TestMetadataSidecarConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(MetadataSidecarConfig.class)
                .setEnabled(false)
                .setExecutablePath(null)
                .setProgramArguments("")
                .setStartupTimeout(new Duration(2, TimeUnit.MINUTES))
                .setStorageOncallName("presto_oncall")
                .setStorageUserName("presto")
                .setStorageServiceName("warm_storage_service"));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("metadata-sidecar.enabled", "true")
                .put("metadata-sidecar.executable-path", "/opt/sapphire_cpp")
                .put("metadata-sidecar.program-arguments", "--logtostderr")
                .put("metadata-sidecar.startup-timeout", "30s")
                .put("metadata-sidecar.storage-oncall-name", "custom_oncall")
                .put("metadata-sidecar.storage-user-name", "custom_user")
                .put("metadata-sidecar.storage-service-name", "custom_svc")
                .build();

        MetadataSidecarConfig expected = new MetadataSidecarConfig()
                .setEnabled(true)
                .setExecutablePath("/opt/sapphire_cpp")
                .setProgramArguments("--logtostderr")
                .setStartupTimeout(new Duration(30, TimeUnit.SECONDS))
                .setStorageOncallName("custom_oncall")
                .setStorageUserName("custom_user")
                .setStorageServiceName("custom_svc");

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
