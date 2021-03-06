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

import com.facebook.drift.transport.netty.codec.Protocol;
import com.facebook.presto.server.InternalCommunicationConfig.CommunicationProtocol;
import com.google.common.collect.ImmutableMap;
import io.airlift.units.DataSize;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static io.airlift.units.DataSize.Unit.MEGABYTE;

public class TestInternalCommunicationConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(InternalCommunicationConfig.class)
                .setHttpsRequired(false)
                .setKeyStorePath(null)
                .setKeyStorePassword(null)
                .setTrustStorePath(null)
                .setTrustStorePassword(null)
                .setKerberosEnabled(false)
                .setIncludedCipherSuites(null)
                .setExcludeCipherSuites(null)
                .setKerberosUseCanonicalHostname(true)
                .setBinaryTransportEnabled(false)
                .setMaxTaskUpdateSize(new DataSize(16, MEGABYTE))
                .setTaskCommunicationProtocol(CommunicationProtocol.HTTP)
                .setServerInfoCommunicationProtocol(CommunicationProtocol.HTTP)
                .setThriftTransportEnabled(false)
                .setThriftProtocol(Protocol.BINARY));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("internal-communication.https.required", "true")
                .put("internal-communication.https.keystore.path", "/a")
                .put("internal-communication.https.trust-store-path", "/a")
                .put("internal-communication.https.trust-store-password", "key")
                .put("internal-communication.https.keystore.key", "key")
                .put("internal-communication.https.included-cipher", "cipher")
                .put("internal-communication.https.excluded-cipher", "")
                .put("internal-communication.kerberos.enabled", "true")
                .put("internal-communication.kerberos.use-canonical-hostname", "false")
                .put("experimental.internal-communication.binary-transport-enabled", "true")
                .put("experimental.internal-communication.max-task-update-size", "512MB")
                .put("internal-communication.task-communication-protocol", "THRIFT")
                .put("internal-communication.server-info-communication-protocol", "THRIFT")
                .put("experimental.internal-communication.thrift-transport-enabled", "true")
                .put("experimental.internal-communication.thrift-transport-protocol", "COMPACT")
                .build();

        InternalCommunicationConfig expected = new InternalCommunicationConfig()
                .setHttpsRequired(true)
                .setKeyStorePath("/a")
                .setKeyStorePassword("key")
                .setTrustStorePath("/a")
                .setTrustStorePassword("key")
                .setIncludedCipherSuites("cipher")
                .setExcludeCipherSuites("")
                .setKerberosEnabled(true)
                .setKerberosUseCanonicalHostname(false)
                .setBinaryTransportEnabled(true)
                .setMaxTaskUpdateSize(new DataSize(512, MEGABYTE))
                .setTaskCommunicationProtocol(CommunicationProtocol.THRIFT)
                .setServerInfoCommunicationProtocol(CommunicationProtocol.THRIFT)
                .setThriftTransportEnabled(true)
                .setThriftProtocol(Protocol.COMPACT);

        assertFullMapping(properties, expected);
    }
}
