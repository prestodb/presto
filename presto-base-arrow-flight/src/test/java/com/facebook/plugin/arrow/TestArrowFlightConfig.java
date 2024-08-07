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
package com.facebook.plugin.arrow;

import com.facebook.airlift.configuration.testing.ConfigAssertions;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

public class TestArrowFlightConfig
{
    @Test
    public void testDefaults()
    {
        ConfigAssertions.assertRecordedDefaults(ConfigAssertions.recordDefaults(ArrowFlightConfig.class)
                .setFlightServerName(null)
                .setVerifyServer(null)
                .setFlightServerSSLCertificate(null)
                .setArrowFlightServerSslEnabled(null)
                .setArrowFlightPort(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("arrow-flight.server", "127.0.0.1")
                .put("arrow-flight.server.verify", "true")
                .put("arrow-flight.server-ssl-certificate", "cert")
                .put("arrow-flight.server-ssl-enabled", "true")
                .put("arrow-flight.server.port", "443")
                .build();

        ArrowFlightConfig expected = new ArrowFlightConfig()
                .setFlightServerName("127.0.0.1")
                .setVerifyServer(true)
                .setFlightServerSSLCertificate("cert")
                .setArrowFlightServerSslEnabled(true)
                .setArrowFlightPort(443);

        ConfigAssertions.assertFullMapping(properties, expected);
    }
}
