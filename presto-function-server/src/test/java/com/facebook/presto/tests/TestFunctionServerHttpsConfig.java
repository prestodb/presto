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
package com.facebook.presto.tests;

import com.facebook.airlift.http.server.HttpServerConfig;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

/**
 * Tests for Function Server configuration properties.
 */
public class TestFunctionServerHttpsConfig
{
    @Test
    public void testFunctionServerMtlsConfiguration()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("http-server.http.enabled", "false")
                .put("http-server.https.enabled", "true")
                .put("http-server.https.port", "9443")
                .put("http-server.https.keystore.path", "<path>/function-server-keystore.jks")
                .put("http-server.https.keystore.key", "changeit")
                .put("http-server.https.truststore.path", "<path>/function-server-truststore.jks")
                .put("http-server.https.truststore.key", "changeit")
                .build();

        HttpServerConfig config;

        // Apply properties using Airlift's configuration system
        com.facebook.airlift.configuration.ConfigurationFactory configFactory =
                new com.facebook.airlift.configuration.ConfigurationFactory(properties);
        config = configFactory.build(HttpServerConfig.class);

        // Verify the configuration
        assertFalse(config.isHttpEnabled(), "HTTP should be disabled");
        assertTrue(config.isHttpsEnabled(), "HTTPS should be enabled");
        assertEquals(config.getHttpsPort(), 9443, "HTTPS port should be 9443");
        assertEquals(config.getKeystorePath(),
                "<path>/function-server-keystore.jks",
                "Keystore path should match");
        assertEquals(config.getKeystorePassword(), "changeit", "Keystore password should match");
        assertEquals(config.getTrustStorePath(),
                "<path>/function-server-truststore.jks",
                "Truststore path should match");
        assertEquals(config.getTrustStorePassword(), "changeit", "Truststore password should match");
    }
}
