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
package com.facebook.presto.iceberg.nessie;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestNessieConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(NessieConfig.class)
                .setClientBuilderImpl(null)
                .setAuthenticationType(null)
                .setBearerToken(null)
                .setCompressionEnabled(true)
                .setDefaultReferenceName("main")
                .setConnectTimeoutMillis(null)
                .setPassword(null)
                .setReadTimeoutMillis(null)
                .setReadTimeoutMillis(null)
                .setServerUri(null)
                .setUsername(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = ImmutableMap.<String, String>builder()
                .put("iceberg.nessie.uri", "http://localhost:xxx/api/v1")
                .put("iceberg.nessie.ref", "someRef")
                .put("iceberg.nessie.auth.type", "BASIC")
                .put("iceberg.nessie.auth.basic.username", "user")
                .put("iceberg.nessie.auth.basic.password", "pass")
                .put("iceberg.nessie.auth.bearer.token", "bearerToken")
                .put("iceberg.nessie.compression-enabled", "false")
                .put("iceberg.nessie.connect-timeout-ms", "123")
                .put("iceberg.nessie.read-timeout-ms", "456")
                .put("iceberg.nessie.client-builder-impl", "org.projectnessie.example.ClientBuilderImpl")
                .build();

        NessieConfig expected = new NessieConfig()
                .setServerUri("http://localhost:xxx/api/v1")
                .setDefaultReferenceName("someRef")
                .setAuthenticationType(AuthenticationType.BASIC)
                .setUsername("user")
                .setPassword("pass")
                .setBearerToken("bearerToken")
                .setCompressionEnabled(false)
                .setConnectTimeoutMillis(123)
                .setReadTimeoutMillis(456)
                .setClientBuilderImpl("org.projectnessie.example.ClientBuilderImpl");

        assertFullMapping(properties, expected);
    }
}
