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
package com.facebook.presto.pinot;

import org.apache.pinot.common.utils.grpc.GrpcQueryClient;
import org.testng.annotations.Test;

import static com.facebook.presto.pinot.PinotConfig.DEFAULT_STREAMING_SERVER_GRPC_MAX_INBOUND_MESSAGE_BYTES;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;

@Test
public class TestPinotPageSourceProvider
{
    @Test
    public void testExtractTlsPropertyDefault()
    {
        PinotConfig config = new PinotConfig();
        GrpcQueryClient.Config extracted = PinotPageSourceProvider.extractGrpcQueryClientConfig(config);
        assertEquals(extracted.getMaxInboundMessageSizeBytes(), DEFAULT_STREAMING_SERVER_GRPC_MAX_INBOUND_MESSAGE_BYTES);
        assertTrue(extracted.isUsePlainText());
        assertNotNull(extracted.getTlsConfig());
        assertNull(extracted.getTlsConfig().getKeyStorePath());
        assertNull(extracted.getTlsConfig().getKeyStorePassword());
        assertNull(extracted.getTlsConfig().getTrustStorePath());
        assertNull(extracted.getTlsConfig().getTrustStorePassword());
    }

    @Test
    public void testExtractTlsPropertySpecified()
    {
        PinotConfig config = new PinotConfig()
                .setGrpcTlsTrustStorePassword("changeit1")
                .setGrpcTlsTrustStoreType("jks-truststore")
                .setGrpcTlsTrustStorePath("/path/to/truststore/file.jks")
                .setGrpcTlsKeyStorePath("/path/to/keystore/file.jks")
                .setGrpcTlsKeyStorePassword("changeit2")
                .setGrpcTlsKeyStoreType("jks-keystore")
                .setUseSecureConnection(true);

        GrpcQueryClient.Config extracted = PinotPageSourceProvider.extractGrpcQueryClientConfig(config);
        assertEquals(extracted.getMaxInboundMessageSizeBytes(), DEFAULT_STREAMING_SERVER_GRPC_MAX_INBOUND_MESSAGE_BYTES);
        assertFalse(extracted.isUsePlainText());
        assertNotNull(extracted.getTlsConfig());
        assertEquals(extracted.getTlsConfig().getKeyStorePath(), "/path/to/keystore/file.jks");
        assertEquals(extracted.getTlsConfig().getKeyStoreType(), "jks-keystore");
        assertEquals(extracted.getTlsConfig().getKeyStorePassword(), "changeit2");
        assertEquals(extracted.getTlsConfig().getTrustStorePath(), "/path/to/truststore/file.jks");
        assertEquals(extracted.getTlsConfig().getTrustStorePassword(), "changeit1");
        assertEquals(extracted.getTlsConfig().getTrustStoreType(), "jks-truststore");
    }
}
