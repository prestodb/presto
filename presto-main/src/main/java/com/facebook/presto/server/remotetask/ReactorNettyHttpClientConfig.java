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
package com.facebook.presto.server.remotetask;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;

import java.util.Optional;

import static com.facebook.airlift.units.DataSize.Unit.KILOBYTE;
import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.concurrent.TimeUnit.SECONDS;

public class ReactorNettyHttpClientConfig
{
    private boolean reactorNettyHttpClientEnabled;
    private boolean httpsEnabled;
    private int minConnections = 50;
    private int maxConnections = 100;
    private int maxStreamPerChannel = 100;
    private int selectorThreadCount = Runtime.getRuntime().availableProcessors();
    private int eventLoopThreadCount = Runtime.getRuntime().availableProcessors();
    private Duration connectTimeout = new Duration(10, SECONDS);
    private Duration requestTimeout = new Duration(10, SECONDS);
    private Duration maxIdleTime = new Duration(0, SECONDS);
    private Duration evictBackgroundTime = new Duration(0, SECONDS);
    private Duration pendingAcquireTimeout = new Duration(0, SECONDS);
    private DataSize maxInitialWindowSize = new DataSize(0, MEGABYTE);
    private DataSize maxFrameSize = new DataSize(0, MEGABYTE);
    private String keyStorePath;
    private String keyStorePassword;
    private String trustStorePath;
    private Optional<String> cipherSuites = Optional.empty();

    private boolean http2CompressionEnabled;
    private DataSize payloadSizeThreshold = new DataSize(50, KILOBYTE);
    private double compressionSavingThreshold = 0.1;
    private DataSize tcpBufferSize = new DataSize(512, KILOBYTE);
    private DataSize writeBufferWaterMarkLow = new DataSize(256, KILOBYTE);
    private DataSize writeBufferWaterMarkHigh = new DataSize(512, KILOBYTE);

    private boolean isHttp2ConnectionPoolStatsTrackingEnabled;
    private boolean isHttp2ClientStatsTrackingEnabled;
    private boolean isChannelOptionSoKeepAliveEnabled = true;
    private boolean isChannelOptionTcpNoDelayEnabled = true;

    public boolean isHttp2ClientStatsTrackingEnabled()
    {
        return isHttp2ClientStatsTrackingEnabled;
    }

    @Config("reactor.enable-http2-client-stats-tracking")
    public ReactorNettyHttpClientConfig setHttp2ClientStatsTrackingEnabled(boolean isHttp2ClientStatsTrackingEnabled)
    {
        this.isHttp2ClientStatsTrackingEnabled = isHttp2ClientStatsTrackingEnabled;
        return this;
    }

    public boolean isHttp2ConnectionPoolStatsTrackingEnabled()
    {
        return isHttp2ConnectionPoolStatsTrackingEnabled;
    }

    @Config("reactor.enable-http2-connection-pool-stats-tracking")
    public ReactorNettyHttpClientConfig setHttp2ConnectionPoolStatsTrackingEnabled(boolean isHttp2ConnectionPoolStatsTrackingEnabled)
    {
        this.isHttp2ConnectionPoolStatsTrackingEnabled = isHttp2ConnectionPoolStatsTrackingEnabled;
        return this;
    }

    public boolean isChannelOptionSoKeepAliveEnabled()
    {
        return isChannelOptionSoKeepAliveEnabled;
    }

    @Config("reactor.channel-option-so-keep-alive")
    public ReactorNettyHttpClientConfig setChannelOptionSoKeepAliveEnabled(boolean isChannelOptionSoKeepAliveEnabled)
    {
        this.isChannelOptionSoKeepAliveEnabled = isChannelOptionSoKeepAliveEnabled;
        return this;
    }

    public boolean isChannelOptionTcpNoDelayEnabled()
    {
        return isChannelOptionTcpNoDelayEnabled;
    }

    @Config("reactor.channel-option-tcp-no-delay")
    public ReactorNettyHttpClientConfig setChannelOptionTcpNoDelayEnabled(boolean isChannelOptionTcpNoDelayEnabled)
    {
        this.isChannelOptionTcpNoDelayEnabled = isChannelOptionTcpNoDelayEnabled;
        return this;
    }

    @Config("reactor.enable-http2-compression")
    public ReactorNettyHttpClientConfig setHttp2CompressionEnabled(boolean http2CompressionEnabled)
    {
        this.http2CompressionEnabled = http2CompressionEnabled;
        return this;
    }

    public boolean isHttp2CompressionEnabled()
    {
        return http2CompressionEnabled;
    }

    public double getCompressionSavingThreshold()
    {
        return compressionSavingThreshold;
    }

    @Config("reactor.compression-ratio-threshold")
    @ConfigDescription("Use compressed data if the compression ratio is above the threshold")
    public ReactorNettyHttpClientConfig setCompressionSavingThreshold(double compressionSavingThreshold)
    {
        this.compressionSavingThreshold = compressionSavingThreshold;
        return this;
    }

    @Min(1024)
    @Max(1024 * 1024)
    public int getTcpBufferSize()
    {
        return (int) tcpBufferSize.toBytes();
    }

    @Config("reactor.tcp-buffer-size")
    public ReactorNettyHttpClientConfig setTcpBufferSize(DataSize tcpBufferSize)
    {
        this.tcpBufferSize = tcpBufferSize;
        return this;
    }

    @Min(1024)
    @Max(1024 * 1024)
    public int getWriteBufferWaterMarkLow()
    {
        return (int) writeBufferWaterMarkLow.toBytes();
    }

    @Config("reactor.tcp-write-buffer-water-mark-low")
    public ReactorNettyHttpClientConfig setWriteBufferWaterMarkLow(DataSize writeBufferWaterMarkLow)
    {
        this.writeBufferWaterMarkLow = writeBufferWaterMarkLow;
        return this;
    }

    @Min(1024)
    @Max(1024 * 1024)
    public int getWriteBufferWaterMarkHigh()
    {
        return (int) writeBufferWaterMarkHigh.toBytes();
    }

    @Config("reactor.tcp-write-buffer-water-mark-high")
    public ReactorNettyHttpClientConfig setWriteBufferWaterMarkHigh(DataSize writeBufferWaterMarkHigh)
    {
        this.writeBufferWaterMarkHigh = writeBufferWaterMarkHigh;
        return this;
    }

    @Min(1024)
    @Max(512 * 1024)
    public int getPayloadSizeThreshold()
    {
        return (int) payloadSizeThreshold.toBytes();
    }

    @Config("reactor.payload-compression-threshold")
    public ReactorNettyHttpClientConfig setPayloadSizeThreshold(DataSize payloadSizeThreshold)
    {
        this.payloadSizeThreshold = payloadSizeThreshold;
        return this;
    }

    public boolean isReactorNettyHttpClientEnabled()
    {
        return reactorNettyHttpClientEnabled;
    }

    @Config("reactor.netty-http-client-enabled")
    @ConfigDescription("Enable reactor netty client for http communication between coordinator and worker")
    public ReactorNettyHttpClientConfig setReactorNettyHttpClientEnabled(boolean reactorNettyHttpClientEnabled)
    {
        this.reactorNettyHttpClientEnabled = reactorNettyHttpClientEnabled;
        return this;
    }

    public boolean isHttpsEnabled()
    {
        return httpsEnabled;
    }

    @Config("reactor.https-enabled")
    public ReactorNettyHttpClientConfig setHttpsEnabled(boolean httpsEnabled)
    {
        this.httpsEnabled = httpsEnabled;
        return this;
    }

    public int getMinConnections()
    {
        return minConnections;
    }

    @Min(10)
    @Config("reactor.min-connections")
    @ConfigDescription("Min number of connections in the pool used by the netty http2 client to talk to the workers")
    public ReactorNettyHttpClientConfig setMinConnections(int minConnections)
    {
        this.minConnections = minConnections;
        return this;
    }

    public int getMaxConnections()
    {
        return maxConnections;
    }

    @Min(10)
    @Config("reactor.max-connections")
    @ConfigDescription("Max total number of connections in the pool used by the netty client to talk to the workers")
    public ReactorNettyHttpClientConfig setMaxConnections(int maxConnections)
    {
        this.maxConnections = maxConnections;
        return this;
    }

    public int getMaxStreamPerChannel()
    {
        return maxStreamPerChannel;
    }

    @Config("reactor.max-stream-per-channel")
    @ConfigDescription("Max number of streams per single tcp connection between coordinator and worker")
    public ReactorNettyHttpClientConfig setMaxStreamPerChannel(int maxStreamPerChannel)
    {
        this.maxStreamPerChannel = maxStreamPerChannel;
        return this;
    }

    public int getSelectorThreadCount()
    {
        return selectorThreadCount;
    }

    @Config("reactor.selector-thread-count")
    @ConfigDescription("Number of select threads used by netty to handle the http messages")
    public ReactorNettyHttpClientConfig setSelectorThreadCount(int selectorThreadCount)
    {
        this.selectorThreadCount = selectorThreadCount;
        return this;
    }

    public int getEventLoopThreadCount()
    {
        return eventLoopThreadCount;
    }

    @Config("reactor.event-loop-thread-count")
    @ConfigDescription("Number of event loop threads used by netty to handle the http messages")
    public ReactorNettyHttpClientConfig setEventLoopThreadCount(int eventLoopThreadCount)
    {
        this.eventLoopThreadCount = eventLoopThreadCount;
        return this;
    }

    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("reactor.connect-timeout")
    public ReactorNettyHttpClientConfig setConnectTimeout(Duration connectTimeout)
    {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("reactor.request-timeout")
    public ReactorNettyHttpClientConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }

    public Duration getMaxIdleTime()
    {
        return maxIdleTime;
    }

    @Config("reactor.max-idle-time")
    public ReactorNettyHttpClientConfig setMaxIdleTime(Duration maxIdleTime)
    {
        this.maxIdleTime = maxIdleTime;
        return this;
    }

    public Duration getEvictBackgroundTime()
    {
        return evictBackgroundTime;
    }

    @Config("reactor.evict-background-time")
    public ReactorNettyHttpClientConfig setEvictBackgroundTime(Duration evictBackgroundTime)
    {
        this.evictBackgroundTime = evictBackgroundTime;
        return this;
    }

    public Duration getPendingAcquireTimeout()
    {
        return pendingAcquireTimeout;
    }

    @Config("reactor.pending-acquire-timeout")
    public ReactorNettyHttpClientConfig setPendingAcquireTimeout(Duration pendingAcquireTimeout)
    {
        this.pendingAcquireTimeout = pendingAcquireTimeout;
        return this;
    }

    public DataSize getMaxInitialWindowSize()
    {
        return maxInitialWindowSize;
    }

    @Config("reactor.max-initial-window-size")
    public ReactorNettyHttpClientConfig setMaxInitialWindowSize(DataSize maxInitialWindowSize)
    {
        this.maxInitialWindowSize = maxInitialWindowSize;
        return this;
    }

    public DataSize getMaxFrameSize()
    {
        return maxFrameSize;
    }

    @Config("reactor.max-frame-size")
    public ReactorNettyHttpClientConfig setMaxFrameSize(DataSize maxFrameSize)
    {
        this.maxFrameSize = maxFrameSize;
        return this;
    }

    public String getKeyStorePath()
    {
        return keyStorePath;
    }

    @Config("reactor.keystore-path")
    public ReactorNettyHttpClientConfig setKeyStorePath(String keyStorePath)
    {
        this.keyStorePath = keyStorePath;
        return this;
    }

    public String getKeyStorePassword()
    {
        return keyStorePassword;
    }

    @Config("reactor.keystore-password")
    public ReactorNettyHttpClientConfig setKeyStorePassword(String keyStorePassword)
    {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public String getTrustStorePath()
    {
        return trustStorePath;
    }

    @Config("reactor.truststore-path")
    public ReactorNettyHttpClientConfig setTrustStorePath(String trustStorePath)
    {
        this.trustStorePath = trustStorePath;
        return this;
    }

    public Optional<String> getCipherSuites()
    {
        return cipherSuites;
    }

    @Config("reactor.cipher-suites")
    public ReactorNettyHttpClientConfig setCipherSuites(String cipherSuites)
    {
        this.cipherSuites = Optional.ofNullable(cipherSuites);
        return this;
    }
}
