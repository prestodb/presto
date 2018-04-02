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
import com.facebook.presto.connector.thrift.api.PrestoThriftId;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableColumnSet;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableSchemaName;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableTableMetadata;
import com.facebook.presto.connector.thrift.api.PrestoThriftNullableToken;
import com.facebook.presto.connector.thrift.api.PrestoThriftPageResult;
import com.facebook.presto.connector.thrift.api.PrestoThriftSchemaTableName;
import com.facebook.presto.connector.thrift.api.PrestoThriftService;
import com.facebook.presto.connector.thrift.api.PrestoThriftServiceException;
import com.facebook.presto.connector.thrift.api.PrestoThriftSplitBatch;
import com.facebook.presto.connector.thrift.api.PrestoThriftTupleDomain;
import com.facebook.presto.connector.thrift.location.HostLocationHandle;
import com.facebook.presto.connector.thrift.location.HostLocationProvider;
import com.facebook.presto.connector.thrift.tracetoken.ThriftTraceToken;
import com.facebook.presto.connector.thrift.tracetoken.ThriftTraceTokenHandler;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.facebook.swift.service.ThriftClient;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import io.airlift.units.Duration;

import javax.inject.Inject;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.connector.thrift.ThriftErrorCode.THRIFT_SERVICE_CONNECTION_ERROR;
import static java.util.Objects.requireNonNull;

public class DefaultPrestoThriftServiceProvider
        implements ConnectedThriftServiceProvider
{
    private final ThriftClient<PrestoThriftService> thriftClient;
    private final HostLocationProvider locationProvider;
    private final long thriftConnectTimeoutMs;
    private final ThriftTraceTokenHandler traceTokenHandler;

    @Inject
    public DefaultPrestoThriftServiceProvider(
            ThriftClient<PrestoThriftService> thriftClient,
            HostLocationProvider locationProvider,
            ThriftTraceTokenHandler traceTokenHandler)
    {
        this.thriftClient = requireNonNull(thriftClient, "thriftClient is null");
        this.locationProvider = requireNonNull(locationProvider, "locationProvider is null");
        this.thriftConnectTimeoutMs = Duration.valueOf(thriftClient.getConnectTimeout()).toMillis();
        this.traceTokenHandler = requireNonNull(traceTokenHandler, "traceTokenHandler is null");
    }

    @Override
    public ConnectedThriftService anyHostClient(Optional<ThriftTraceToken> traceToken)
    {
        HostLocationHandle hostLocationHandle = locationProvider.getAnyHost();
        PrestoThriftService prestoThriftService = connectTo(hostLocationHandle.getHostAddress(), traceToken);
        return new DefaultThriftService(prestoThriftService, hostLocationHandle);
    }

    @Override
    public ConnectedThriftService selectedHostClient(List<HostAddress> hosts, Optional<ThriftTraceToken> traceToken)
    {
        HostLocationHandle hostLocationHandle = locationProvider.getAnyOf(hosts);
        PrestoThriftService prestoThriftService = connectTo(hostLocationHandle.getHostAddress(), traceToken);
        return new DefaultThriftService(prestoThriftService, hostLocationHandle);
    }

    private PrestoThriftService connectTo(HostAddress host, Optional<ThriftTraceToken> traceToken)
    {
        try {
            PrestoThriftService client = thriftClient.open(new FramedClientConnector(HostAndPort.fromParts(host.getHostText(), host.getPort())))
                    .get(thriftConnectTimeoutMs, TimeUnit.MILLISECONDS);
            traceToken.ifPresent(traceTokenValue -> traceTokenHandler.applyTraceToken(client, traceTokenValue));
            return client;
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Interrupted while connecting to thrift host at " + host, e);
        }
        catch (ExecutionException | TimeoutException e) {
            throw new PrestoException(THRIFT_SERVICE_CONNECTION_ERROR, "Cannot connect to thrift host at " + host, e);
        }
    }

    private static final class DefaultThriftService
            implements ConnectedThriftService
    {
        private final PrestoThriftService prestoThriftService;
        private final HostLocationHandle hostLocationHandle;

        public DefaultThriftService(PrestoThriftService prestoThriftService, HostLocationHandle hostLocationHandle)
        {
            this.prestoThriftService = requireNonNull(prestoThriftService, "prestoThriftService is null");
            this.hostLocationHandle = requireNonNull(hostLocationHandle, "hostLocationHandle is null");
        }

        @Override
        public List<String> listSchemaNames()
                throws PrestoThriftServiceException
        {
            return prestoThriftService.listSchemaNames();
        }

        @Override
        public List<PrestoThriftSchemaTableName> listTables(PrestoThriftNullableSchemaName schemaNameOrNull)
                throws PrestoThriftServiceException
        {
            return prestoThriftService.listTables(schemaNameOrNull);
        }

        @Override
        public PrestoThriftNullableTableMetadata getTableMetadata(PrestoThriftSchemaTableName schemaTableName)
                throws PrestoThriftServiceException
        {
            return prestoThriftService.getTableMetadata(schemaTableName);
        }

        @Override
        public ListenableFuture<PrestoThriftSplitBatch> getSplits(PrestoThriftSchemaTableName schemaTableName, PrestoThriftNullableColumnSet desiredColumns, PrestoThriftTupleDomain outputConstraint, int maxSplitCount, PrestoThriftNullableToken nextToken)
                throws PrestoThriftServiceException
        {
            return prestoThriftService.getSplits(schemaTableName, desiredColumns, outputConstraint, maxSplitCount, nextToken);
        }

        @Override
        public ListenableFuture<PrestoThriftPageResult> getRows(PrestoThriftId splitId, List<String> columns, long maxBytes, PrestoThriftNullableToken nextToken)
                throws PrestoThriftServiceException
        {
            return prestoThriftService.getRows(splitId, columns, maxBytes, nextToken);
        }

        @Override
        public ListenableFuture<PrestoThriftSplitBatch> getIndexSplits(PrestoThriftSchemaTableName schemaTableName, List<String> indexColumnNames, List<String> outputColumnNames, PrestoThriftPageResult keys, PrestoThriftTupleDomain outputConstraint, int maxSplitCount, PrestoThriftNullableToken nextToken)
                throws PrestoThriftServiceException
        {
            return prestoThriftService.getIndexSplits(schemaTableName, indexColumnNames, outputColumnNames, keys, outputConstraint, maxSplitCount, nextToken);
        }

        @Override
        public void close()
        {
            prestoThriftService.close();
        }

        @Override
        public HostLocationHandle getHostLocationHandle()
        {
            return hostLocationHandle;
        }
    }
}
