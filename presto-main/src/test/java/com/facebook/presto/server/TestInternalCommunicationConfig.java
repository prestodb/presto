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
package com.facebook.presto.server;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static io.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static io.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static io.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestInternalCommunicationConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(InternalCommunicationConfig.class)
                .setHttpsRequired(false)
                .setKeyStorePath(null)
                .setKeyStorePassword(null)
                .setKerberosEnabled(false)
                .setKerberosUseCanonicalHostname(true)
                .setBinaryTransportEnabled(false));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("internal-communication.https.required", "true")
                .put("internal-communication.https.keystore.path", "/a")
                .put("internal-communication.https.keystore.key", "key")
                .put("internal-communication.kerberos.enabled", "true")
                .put("internal-communication.kerberos.use-canonical-hostname", "false")
                .put("experimental.internal-communication.binary-transport-enabled", "true")
                .build();

        InternalCommunicationConfig expected = new InternalCommunicationConfig()
                .setHttpsRequired(true)
                .setKeyStorePath("/a")
                .setKeyStorePassword("key")
                .setKerberosEnabled(true)
                .setKerberosUseCanonicalHostname(false)
                .setBinaryTransportEnabled(true);

        assertFullMapping(properties, expected);
    }
}
