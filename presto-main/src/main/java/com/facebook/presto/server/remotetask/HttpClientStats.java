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

import com.facebook.airlift.stats.CounterStat;
import com.facebook.airlift.stats.DistributionStat;
import com.facebook.airlift.stats.TimeStat;
import com.google.inject.Singleton;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;
import reactor.netty.http.client.ContextAwareHttpClientMetricsRecorder;
import reactor.util.context.ContextView;

import java.net.SocketAddress;
import java.time.Duration;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

@Singleton
public class HttpClientStats
        extends ContextAwareHttpClientMetricsRecorder
{
    // HTTP level metrics
    private final TimeStat responseTime = new TimeStat();
    private final TimeStat dataReceivedTime = new TimeStat();
    private final TimeStat dataSentTime = new TimeStat();
    private final CounterStat errorsCount = new CounterStat();
    private final CounterStat bytesReceived = new CounterStat();
    private final CounterStat bytesSent = new CounterStat();
    private final DistributionStat payloadSize = new DistributionStat();

    // Channel level metrics
    private final TimeStat connectTime = new TimeStat();
    private final TimeStat tlsHandshakeTime = new TimeStat();
    private final TimeStat resolveAddressTime = new TimeStat();
    private final CounterStat channelErrorsCount = new CounterStat();
    private final CounterStat channelBytesReceived = new CounterStat();
    private final CounterStat channelBytesSent = new CounterStat();
    private final CounterStat connectionsOpened = new CounterStat();
    private final CounterStat connectionsClosed = new CounterStat();

    // HTTP level metrics recording

    /**
     * Records the time that is spent in consuming incoming data
     *
     * @param contextView The current {@link ContextView} associated with the Mono/Flux
     * @param remoteAddress The remote peer
     * @param uri The requested URI
     * @param method The HTTP method
     * @param status The HTTP status
     * @param time The time in nanoseconds that is spent in consuming incoming data
     */
    @Override
    public void recordDataReceivedTime(
            ContextView contextView,
            SocketAddress remoteAddress,
            String uri,
            String method,
            String status,
            Duration time)
    {
        dataReceivedTime.add(time.toMillis(), MILLISECONDS);
    }

    /**
     * Records the time that is spent in sending outgoing data
     *
     * @param contextView The current {@link ContextView} associated with the Mono/Flux
     * @param remoteAddress The remote peer
     * @param uri The requested URI
     * @param method The HTTP method
     * @param time The time in nanoseconds that is spent in sending outgoing data
     */
    @Override
    public void recordDataSentTime(
            ContextView contextView,
            SocketAddress remoteAddress,
            String uri,
            String method,
            Duration time)
    {
        dataSentTime.add(time.toMillis(), MILLISECONDS);
    }

    /**
     * Records the total time for the request/response
     *
     * @param contextView The current {@link ContextView} associated with the Mono/Flux
     * @param remoteAddress The remote peer
     * @param uri The requested URI
     * @param method The HTTP method
     * @param status The HTTP status
     * @param time The total time in nanoseconds for the request/response
     */
    @Override
    public void recordResponseTime(
            ContextView contextView,
            SocketAddress remoteAddress,
            String uri,
            String method,
            String status,
            Duration time)
    {
        responseTime.add(time.toMillis(), MILLISECONDS);
    }

    /**
     * Increments the number of the errors that are occurred
     *
     * @param contextView The current {@link ContextView} associated with the Mono/Flux
     * @param remoteAddress The remote peer
     * @param uri The requested URI
     */
    @Override
    public void incrementErrorsCount(ContextView contextView, SocketAddress remoteAddress, String uri)
    {
        errorsCount.update(1);
    }

    /**
     * Records the amount of the data that is received, in bytes
     *
     * @param contextView The current {@link ContextView} associated with the Mono/Flux
     * @param remoteAddress The remote peer
     * @param uri The requested URI
     * @param bytes The amount of the data that is received, in bytes
     */
    @Override
    public void recordDataReceived(ContextView contextView, SocketAddress remoteAddress, String uri, long bytes)
    {
        bytesReceived.update(bytes);
    }

    /**
     * Records the amount of the data that is sent, in bytes
     *
     * @param contextView The current {@link ContextView} associated with the Mono/Flux
     * @param remoteAddress The remote peer
     * @param uri The requested URI
     * @param bytes The amount of the data that is sent, in bytes
     */
    @Override
    public void recordDataSent(ContextView contextView, SocketAddress remoteAddress, String uri, long bytes)
    {
        bytesSent.update(bytes);
        payloadSize.add(bytes);
    }

    // Channel level metrics recording

    /**
     * Increments the number of the errors that are occurred
     *
     * @param contextView The current {@link ContextView} associated with the Mono/Flux pipeline
     * @param remoteAddress The remote peer
     */
    @Override
    public void incrementErrorsCount(ContextView contextView, SocketAddress remoteAddress)
    {
        channelErrorsCount.update(1);
    }

    /**
     * Records the time that is spent for connecting to the remote address Relevant only when on the
     * client
     *
     * @param contextView The current {@link ContextView} associated with the Mono/Flux pipeline
     * @param remoteAddress The remote peer
     * @param time The time in nanoseconds that is spent for connecting to the remote address
     * @param status The status of the operation
     */
    @Override
    public void recordConnectTime(
            ContextView contextView,
            SocketAddress remoteAddress,
            Duration time,
            String status)
    {
        connectTime.add(time.toMillis(), MILLISECONDS);
    }

    /**
     * Records the amount of the data that is received, in bytes
     *
     * @param contextView The current {@link ContextView} associated with the Mono/Flux pipeline
     * @param remoteAddress The remote peer
     * @param bytes The amount of the data that is received, in bytes
     */
    @Override
    public void recordDataReceived(ContextView contextView, SocketAddress remoteAddress, long bytes)
    {
        channelBytesReceived.update(bytes);
    }

    /**
     * Records the amount of the data that is sent, in bytes
     *
     * @param contextView The current {@link ContextView} associated with the Mono/Flux pipeline
     * @param remoteAddress The remote peer
     * @param bytes The amount of the data that is sent, in bytes
     */
    @Override
    public void recordDataSent(ContextView contextView, SocketAddress remoteAddress, long bytes)
    {
        channelBytesSent.update(bytes);
    }

    /**
     * Records the time that is spent for TLS handshake
     *
     * @param contextView The current {@link ContextView} associated with the Mono/Flux pipeline
     * @param remoteAddress The remote peer
     * @param time The time in nanoseconds that is spent for TLS handshake
     * @param status The status of the operation
     */
    @Override
    public void recordTlsHandshakeTime(
            ContextView contextView,
            SocketAddress remoteAddress,
            Duration time,
            String status)
    {
        tlsHandshakeTime.add(time.toMillis(), MILLISECONDS);
    }

    /**
     * Records the time that is spent for resolving the remote address Relevant only when on the
     * client
     *
     * @param remoteAddress The remote peer
     * @param time the time in nanoseconds that is spent for resolving to the remote address
     * @param status the status of the operation
     */
    @Override
    public void recordResolveAddressTime(SocketAddress remoteAddress, Duration time, String status)
    {
        resolveAddressTime.add(time.toMillis(), MILLISECONDS);
    }

    /**
     * Records a just accepted server connection
     *
     * @param localAddress the server local address
     * @since 1.0.15
     */
    @Override
    public void recordServerConnectionOpened(SocketAddress localAddress)
    {
        connectionsOpened.update(1);
    }

    /**
     * Records a just disconnected server connection
     *
     * @param localAddress the server local address
     * @since 1.0.15
     */
    @Override
    public void recordServerConnectionClosed(SocketAddress localAddress)
    {
        connectionsClosed.update(1);
    }

    // JMX exposed metrics

    @Managed
    @Nested
    public TimeStat getResponseTime()
    {
        return responseTime;
    }

    @Managed
    @Nested
    public TimeStat getDataReceivedTime()
    {
        return dataReceivedTime;
    }

    @Managed
    @Nested
    public TimeStat getDataSentTime()
    {
        return dataSentTime;
    }

    @Managed
    @Nested
    public CounterStat getErrorsCount()
    {
        return errorsCount;
    }

    @Managed
    @Nested
    public CounterStat getBytesReceived()
    {
        return bytesReceived;
    }

    @Managed
    @Nested
    public CounterStat getBytesSent()
    {
        return bytesSent;
    }

    @Managed
    @Nested
    public DistributionStat getPayloadSize()
    {
        return payloadSize;
    }

    @Managed
    @Nested
    public TimeStat getConnectTime()
    {
        return connectTime;
    }

    @Managed
    @Nested
    public TimeStat getTlsHandshakeTime()
    {
        return tlsHandshakeTime;
    }

    @Managed
    @Nested
    public TimeStat getResolveAddressTime()
    {
        return resolveAddressTime;
    }

    @Managed
    @Nested
    public CounterStat getChannelErrorsCount()
    {
        return channelErrorsCount;
    }

    @Managed
    @Nested
    public CounterStat getChannelBytesReceived()
    {
        return channelBytesReceived;
    }

    @Managed
    @Nested
    public CounterStat getChannelBytesSent()
    {
        return channelBytesSent;
    }

    @Managed
    @Nested
    public CounterStat getConnectionsOpened()
    {
        return connectionsOpened;
    }

    @Managed
    @Nested
    public CounterStat getConnectionsClosed()
    {
        return connectionsClosed;
    }
}
