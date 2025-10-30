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
package com.facebook.presto.hive.azure;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestHiveAzureConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(HiveAzureConfig.class)
                .setWasbStorageAccount(null)
                .setWasbAccessKey(null)
                .setAbfsStorageAccount(null)
                .setAbfsAccessKey(null)
                .setAbfsOAuthClientEndpoint(null)
                .setAbfsOAuthClientId(null)
                .setAbfsOAuthClientSecret(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("hive.azure.wasb-storage-account", "wasb-test-account")
                .put("hive.azure.wasb-access-key", "wasb-test-key")
                .put("hive.azure.abfs-storage-account", "abfs-test-account")
                .put("hive.azure.abfs-access-key", "abfs-test-key")
                .put("hive.azure.abfs.oauth.endpoint", "https://login.microsoftonline.com/tenant-id/oauth2/token")
                .put("hive.azure.abfs.oauth.client-id", "test-client-id")
                .put("hive.azure.abfs.oauth.secret", "test-client-secret")
                .build();

        HiveAzureConfig expected = new HiveAzureConfig()
                .setWasbStorageAccount("wasb-test-account")
                .setWasbAccessKey("wasb-test-key")
                .setAbfsStorageAccount("abfs-test-account")
                .setAbfsAccessKey("abfs-test-key")
                .setAbfsOAuthClientEndpoint("https://login.microsoftonline.com/tenant-id/oauth2/token")
                .setAbfsOAuthClientId("test-client-id")
                .setAbfsOAuthClientSecret("test-client-secret");

        assertFullMapping(properties, expected);
    }
}
