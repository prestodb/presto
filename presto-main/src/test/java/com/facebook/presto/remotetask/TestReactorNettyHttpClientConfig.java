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

import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.server.remotetask.ReactorNettyHttpClientConfig;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertFullMapping;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.assertRecordedDefaults;
import static com.facebook.airlift.configuration.testing.ConfigAssertions.recordDefaults;
import static com.facebook.airlift.units.DataSize.Unit.KILOBYTE;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class TestReactorNettyHttpClientConfig
{
    @Test
    public void testDefaults()
    {
        assertRecordedDefaults(recordDefaults(ReactorNettyHttpClientConfig.class)
                .setReactorNettyHttpClientEnabled(false)
                .setHttpsEnabled(false)
                .setMinConnections(50)
                .setMaxConnections(100)
                .setMaxStreamPerChannel(100)
                .setSelectorThreadCount(Runtime.getRuntime().availableProcessors())
                .setEventLoopThreadCount(Runtime.getRuntime().availableProcessors())
                .setConnectTimeout(new Duration(10, SECONDS))
                .setRequestTimeout(new Duration(10, SECONDS))
                .setMaxIdleTime(new Duration(0, SECONDS))
                .setEvictBackgroundTime(new Duration(0, SECONDS))
                .setPendingAcquireTimeout(new Duration(0, SECONDS))
                .setMaxInitialWindowSize(new DataSize(0, MEGABYTE))
                .setMaxFrameSize(new DataSize(0, MEGABYTE))
                .setKeyStorePath(null)
                .setKeyStorePassword(null)
                .setTrustStorePath(null)
                .setCipherSuites(null)
                .setHttp2CompressionEnabled(false)
                .setPayloadSizeThreshold(new DataSize(50, KILOBYTE))
                .setCompressionSavingThreshold(0.1)
                .setTcpBufferSize(new DataSize(512, KILOBYTE))
                .setWriteBufferWaterMarkHigh(new DataSize(512, KILOBYTE))
                .setWriteBufferWaterMarkLow(new DataSize(256, KILOBYTE))
                .setHttp2ConnectionPoolStatsTrackingEnabled(false)
                .setHttp2ClientStatsTrackingEnabled(false)
                .setChannelOptionSoKeepAliveEnabled(true)
                .setChannelOptionTcpNoDelayEnabled(true));
    }

    @Test
    public void testExplicitPropertyMappings()
    {
        Map<String, String> properties = new ImmutableMap.Builder<String, String>()
                .put("reactor.netty-http-client-enabled", "true")
                .put("reactor.https-enabled", "true")
                .put("reactor.min-connections", "100")
                .put("reactor.max-connections", "500")
                .put("reactor.max-stream-per-channel", "300")
                .put("reactor.selector-thread-count", "50")
                .put("reactor.event-loop-thread-count", "150")
                .put("reactor.connect-timeout", "2s")
                .put("reactor.request-timeout", "1s")
                .put("reactor.max-idle-time", "120s")
                .put("reactor.evict-background-time", "120s")
                .put("reactor.pending-acquire-timeout", "10s")
                .put("reactor.max-initial-window-size", "10MB")
                .put("reactor.max-frame-size", "4MB")
                .put("reactor.keystore-path", "/var/abc/def/presto.jks")
                .put("reactor.truststore-path", "/var/abc/def/presto.jks")
                .put("reactor.keystore-password", "password")
                .put("reactor.cipher-suites", "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256")
                .put("reactor.enable-http2-compression", "true")
                .put("reactor.payload-compression-threshold", "10kB")
                .put("reactor.compression-ratio-threshold", "0.2")
                .put("reactor.tcp-buffer-size", "256kB")
                .put("reactor.tcp-write-buffer-water-mark-high", "256kB")
                .put("reactor.tcp-write-buffer-water-mark-low", "128kB")
                .put("reactor.enable-http2-connection-pool-stats-tracking", "true")
                .put("reactor.enable-http2-client-stats-tracking", "true")
                .put("reactor.channel-option-so-keep-alive", "false")
                .put("reactor.channel-option-tcp-no-delay", "false")
                .build();

        ReactorNettyHttpClientConfig expected = new ReactorNettyHttpClientConfig()
                .setReactorNettyHttpClientEnabled(true)
                .setHttpsEnabled(true)
                .setMinConnections(100)
                .setMaxConnections(500)
                .setMaxStreamPerChannel(300)
                .setSelectorThreadCount(50)
                .setEventLoopThreadCount(150)
                .setConnectTimeout(new Duration(2, SECONDS))
                .setRequestTimeout(new Duration(1, SECONDS))
                .setMaxIdleTime(new Duration(120, SECONDS))
                .setEvictBackgroundTime(new Duration(120, SECONDS))
                .setPendingAcquireTimeout(new Duration(10, SECONDS))
                .setMaxInitialWindowSize(new DataSize(10, MEGABYTE)) // 10MB
                .setMaxFrameSize(new DataSize(4, MEGABYTE)) // 4MB
                .setKeyStorePath("/var/abc/def/presto.jks")
                .setTrustStorePath("/var/abc/def/presto.jks")
                .setKeyStorePassword("password")
                .setCipherSuites("TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256")
                .setHttp2CompressionEnabled(true)
                .setPayloadSizeThreshold(new DataSize(10, KILOBYTE))
                .setCompressionSavingThreshold(0.2)
                .setTcpBufferSize(new DataSize(256, KILOBYTE))
                .setWriteBufferWaterMarkHigh(new DataSize(256, KILOBYTE))
                .setWriteBufferWaterMarkLow(new DataSize(128, KILOBYTE))
                .setHttp2ConnectionPoolStatsTrackingEnabled(true)
                .setHttp2ClientStatsTrackingEnabled(true)
                .setChannelOptionSoKeepAliveEnabled(false)
                .setChannelOptionTcpNoDelayEnabled(false);

        assertFullMapping(properties, expected);
    }
}
