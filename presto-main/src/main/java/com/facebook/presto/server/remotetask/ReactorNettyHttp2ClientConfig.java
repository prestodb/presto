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

import javax.validation.constraints.Min;

import java.util.Optional;

public class ReactorNettyHttp2ClientConfig
{
    private boolean reactorNettyHttp2ClientEnabled;
    private int minConnections = 10;
    private int maxConnections = 100;
    private int maxStreamPerChannel = 100;
    private int eventLoopThreadCount = 50;

    private String keyStorePath;
    private String keyStorePassword;
    private String trustStorePath;
    private Optional<String> cipherSuites = Optional.empty();

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

    public int getMinConnections()
    {
        return minConnections;
    }

    @Min(10)
    @Config("reactor.min-connections")
    @ConfigDescription("Min number of connections in the pool used by the netty client to talk to the workers")
    public ReactorNettyHttp2ClientConfig setMinConnections(int minConnections)
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

    public String getKeyStorePath()
    {
        return keyStorePath;
    }

    @Config("reactor.keystore-path")
    public ReactorNettyHttp2ClientConfig setKeyStorePath(String keyStorePath)
    {
        this.keyStorePath = keyStorePath;
        return this;
    }

    public String getKeyStorePassword()
    {
        return keyStorePassword;
    }

    @Config("reactor.keystore-password")
    public ReactorNettyHttp2ClientConfig setKeyStorePassword(String keyStorePassword)
    {
        this.keyStorePassword = keyStorePassword;
        return this;
    }

    public String getTrustStorePath()
    {
        return trustStorePath;
    }

    @Config("reactor.truststore-path")
    public ReactorNettyHttp2ClientConfig setTrustStorePath(String trustStorePath)
    {
        this.trustStorePath = trustStorePath;
        return this;
    }

    public Optional<String> getCipherSuites()
    {
        return cipherSuites;
    }

    @Config("reactor.cipher-suites")
    public ReactorNettyHttp2ClientConfig setCipherSuites(String cipherSuites)
    {
        this.cipherSuites = Optional.ofNullable(cipherSuites);
        return this;
    }
}
