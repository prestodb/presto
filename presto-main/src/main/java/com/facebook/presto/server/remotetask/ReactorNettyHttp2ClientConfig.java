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
import io.airlift.units.Duration;

import javax.validation.constraints.Min;

import static java.util.concurrent.TimeUnit.SECONDS;

public class ReactorNettyHttp2ClientConfig
{
    private boolean reactorNettyHttp2ClientEnabled;
    private int minConnections = 10;
    private int maxConnections = 100;
    private int maxStreamPerChannel = 100;
    private int selectorThreadCount = Runtime.getRuntime().availableProcessors();
    private int eventLoopThreadCount = Runtime.getRuntime().availableProcessors();
    private Duration connectTimeout = new Duration(10, SECONDS);
    private Duration requestTimeout = new Duration(10, SECONDS);

    public boolean isReactorNettyHttp2ClientEnabled()
    {
        return reactorNettyHttp2ClientEnabled;
    }

    @Config("reactor.netty-http2-client-enabled")
    @ConfigDescription("Enable reactor netty client for http2 communication between coordinator and worker")
    public ReactorNettyHttp2ClientConfig setReactorNettyHttp2ClientEnabled(boolean reactorNettyHttp2ClientEnabled)
    {
        this.reactorNettyHttp2ClientEnabled = reactorNettyHttp2ClientEnabled;
        return this;
    }

    public int getMaxConnections()
    {
        return maxConnections;
    }

    @Min(10)
    @Config("reactor.max-connections")
    @ConfigDescription("Max total number of connections in the pool used by the netty client to talk to the workers")
    public ReactorNettyHttp2ClientConfig setMaxConnections(int maxConnections)
    {
        this.maxConnections = maxConnections;
        return this;
    }

    public int getMaxStreamPerChannel()
    {
        return maxStreamPerChannel;
    }

    @Config("reactor.max-stream-per-channel")
    @ConfigDescription("Max number of streams per single HTTP2 connection between coordinator and worker")
    public ReactorNettyHttp2ClientConfig setMaxStreamPerChannel(int maxStreamPerChannel)
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
    public ReactorNettyHttp2ClientConfig setSelectorThreadCount(int selectorThreadCount)
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
    public ReactorNettyHttp2ClientConfig setEventLoopThreadCount(int eventLoopThreadCount)
    {
        this.eventLoopThreadCount = eventLoopThreadCount;
        return this;
    }

    public Duration getConnectTimeout()
    {
        return connectTimeout;
    }

    @Config("reactor.connect-timeout")
    public ReactorNettyHttp2ClientConfig setConnectTimeout(Duration connectTimeout)
    {
        this.connectTimeout = connectTimeout;
        return this;
    }

    public Duration getRequestTimeout()
    {
        return requestTimeout;
    }

    @Config("reactor.request-timeout")
    public ReactorNettyHttp2ClientConfig setRequestTimeout(Duration requestTimeout)
    {
        this.requestTimeout = requestTimeout;
        return this;
    }
}
