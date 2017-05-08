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
package com.facebook.presto.connector.thrift.clientproviders;

import com.facebook.nifty.client.FramedClientConnector;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.location.HostLocationProvider;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.swift.service.ThriftClient;
import com.google.common.net.HostAndPort;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.connector.thrift.ThriftErrorCode.THRIFT_SERVICE_CONNECTION_ERROR;
import static java.util.Objects.requireNonNull;

public class DefaultPrestoThriftServiceProvider
        implements PrestoThriftServiceProvider
{
    private final ThriftClient<PrestoThriftService> thriftClient;
    private final HostLocationProvider locationProvider;
    private final long thriftConnectTimeoutMs;

    @Inject
    public DefaultPrestoThriftServiceProvider(ThriftClient<PrestoThriftService> thriftClient, HostLocationProvider locationProvider)
    {
        this.thriftClient = requireNonNull(thriftClient, "thriftClient is null");
        this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
        this.thriftConnectTimeoutMs = Duration.valueOf(thriftClient.getConnectTimeout()).toMillis();
    }

    @Override
    public PrestoThriftService anyHostClient()
    {
        return connectTo(locationProvider.getAnyHost());
    }

    @Override
    public PrestoThriftService selectedHostClient(List<HostAddress> hosts)
    {
        return connectTo(locationProvider.getAnyOf(hosts));
    }

    private PrestoThriftService connectTo(HostAddress host)
    {
        try {
            return thriftClient.open(new FramedClientConnector(HostAndPort.fromParts(host.getHostText(), host.getPort())))
                    .get(thriftConnectTimeoutMs, TimeUnit.MILLISECONDS);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while connecting to thrift host at " + host, e);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new PrestoException(THRIFT_SERVICE_CONNECTION_ERROR, "Cannot connect to thrift host at " + host, e);
        }
    }
}
