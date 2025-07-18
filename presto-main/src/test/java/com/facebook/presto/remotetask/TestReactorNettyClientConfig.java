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
package com.facebook.presto.remotetask;

import com.facebook.presto.server.remotetask.ReactorNettyHttp2ClientConfig;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;

public class TestReactorNettyClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ReactorNettyHttp2ClientConfig.class)
                .setReactorNettyHttp2ClientEnabled(false)
                .setMinConnections(10)
                .setMaxConnections(100)
                .setMaxStreamPerChannel(100)
                .setEventLoopThreadCount(50)
                .setKeyStorePath(null)
                .setKeyStorePassword(null)
                .setTrustStorePath(null)
                .setCipherSuites(null));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("reactor.netty-http2-client-enabled", "true")
                .put("reactor.min-connections", "200")
                .put("reactor.max-connections", "500")
                .put("reactor.max-stream-per-channel", "300")
                .put("reactor.event-loop-thread-count", "150")
                .put("reactor.keystore-path", "/var/abc/x509_identities/client.pem")
                .put("reactor.keystore-password", "password")
                .put("reactor.truststore-path", "/var/abc/rootcanal/ca.pem")
                .put("reactor.cipher-suites", "TLS_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256")
                .build();

        ReactorNettyHttp2ClientConfig expected = new ReactorNettyHttp2ClientConfig()
                .setReactorNettyHttp2ClientEnabled(true)
                .setMinConnections(200)
                .setMaxConnections(500)
                .setMaxStreamPerChannel(300)
                .setEventLoopThreadCount(150)
                .setKeyStorePath("/var/abc/x509_identities/client.pem")
                .setKeyStorePassword("password")
                .setTrustStorePath("/var/abc/rootcanal/ca.pem")
                .setCipherSuites("TLS_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256");

        assertFullMapping(properties, expected);
    }
}
