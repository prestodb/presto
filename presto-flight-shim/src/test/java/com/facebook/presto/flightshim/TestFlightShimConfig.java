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
package com.facebook.presto.flightshim;

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestFlightShimConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(FlightShimConfig.class)
                .setServerName(null)
                .setServerPort(null)
                .setServerSslEnabled(true)
                .setServerSSLCertificateFile(null)
                .setServerSSLKeyFile(null)
                .setReadSplitThreadPoolSize(16)
                .setClientSSLCertificateFile(null)
                .setClientSSLKeyFile(null)
                .setMaxRowsPerBatch(10000));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("server", "localhost")
                .put("server.port", "9432")
                .put("server-ssl-enabled", "false")
                .put("server-ssl-certificate-file", "/some/path/server.cert")
                .put("server-ssl-key-file", "/some/path/server.key")
                .put("client-ssl-certificate-file", "/some/path/client.cert")
                .put("client-ssl-key-file", "/some/path/client.key")
                .put("thread-pool-size", "8")
                .put("max-rows-per-batch", "1000")
                .build();

        FlightShimConfig expected = new FlightShimConfig()
                .setServerName("localhost")
                .setServerPort(9432)
                .setServerSslEnabled(false)
                .setServerSSLCertificateFile("/some/path/server.cert")
                .setServerSSLKeyFile("/some/path/server.key")
                .setClientSSLCertificateFile("/some/path/client.cert")
                .setClientSSLKeyFile("/some/path/client.key")
                .setReadSplitThreadPoolSize(8)
                .setMaxRowsPerBatch(1000);

        assertFullMapping(properties, expected);
    }
}
