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

import com.facebook.presto.connector.thrift.ThriftConnectorConfig;
import com.facebook.presto.connector.thrift.annotations.ForRetryDriver;
import com.facebook.presto.connector.thrift.annotations.NonRetrying;
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
import com.facebook.presto.connector.thrift.util.RetryDriver;
import com.facebook.presto.spi.HostAddress;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import io.airlift.log.Logger;

import javax.annotation.concurrent.NotThreadSafe;
import javax.inject.Inject;

import java.util.List;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public class RetryingPrestoThriftServiceProvider
        implements PrestoThriftServiceProvider
{
    private static final Logger log = Logger.get(RetryingPrestoThriftServiceProvider.class);
    private final PrestoThriftServiceProvider original;
    private final RetryDriver retry;

    @Inject
    public RetryingPrestoThriftServiceProvider(@NonRetrying PrestoThriftServiceProvider original, @ForRetryDriver ListeningScheduledExecutorService retryExecutor, ThriftConnectorConfig config)
    {
        this.original = requireNonNull(original, "original is null");
        requireNonNull(retryExecutor, "retryExecutor is null");
        requireNonNull(config, "config is null");

        retry = RetryDriver.retry(retryExecutor)
                .maxAttempts(config.getMaxRetryAttempts())
                .stopRetryingWhen(e -> e instanceof PrestoThriftServiceException && !((PrestoThriftServiceException) e).isRetryable())
                .exponentialBackoff(
                        config.getMinRetrySleepTime(),
                        config.getMaxRetrySleepTime(),
                        config.getMaxRetryDuration(),
                        config.getRetryScaleFactor());
    }

    @Override
    public PrestoThriftService anyHostClient()
    {
        return new RetryingService(original::anyHostClient, retry);
    }

    @Override
    public PrestoThriftService selectedHostClient(List<HostAddress> hosts)
    {
        return new RetryingService(() -> original.selectedHostClient(hosts), retry);
    }

    @NotThreadSafe
    private static final class RetryingService
            implements PrestoThriftService
    {
        private final Supplier<PrestoThriftService> clientSupplier;
        private final RetryDriver retry;
        private PrestoThriftService client;

        public RetryingService(Supplier<PrestoThriftService> clientSupplier, RetryDriver retry)
        {
            this.clientSupplier = requireNonNull(clientSupplier, "clientSupplier is null");
            this.retry = retry.onRetry(this::close);
        }

        private PrestoThriftService getClient()
        {
            if (client != null) {
                return client;
            }
            client = clientSupplier.get();
            return client;
        }

        @Override
        public List<String> listSchemaNames()
        {
            return retry.run("listSchemaNames", () -> getClient().listSchemaNames());
        }

        @Override
        public List<PrestoThriftSchemaTableName> listTables(PrestoThriftNullableSchemaName schemaNameOrNull)
        {
            return retry.run("listTables", () -> getClient().listTables(schemaNameOrNull));
        }

        @Override
        public PrestoThriftNullableTableMetadata getTableMetadata(PrestoThriftSchemaTableName schemaTableName)
        {
            return retry.run("getTableMetadata", () -> getClient().getTableMetadata(schemaTableName));
        }

        @Override
        public ListenableFuture<PrestoThriftSplitBatch> getSplits(
                PrestoThriftSchemaTableName schemaTableName,
                PrestoThriftNullableColumnSet desiredColumns,
                PrestoThriftTupleDomain outputConstraint,
                int maxSplitCount,
                PrestoThriftNullableToken nextToken)
                throws PrestoThriftServiceException
        {
            return retry.runAsync("getSplits", () -> getClient().getSplits(schemaTableName, desiredColumns, outputConstraint, maxSplitCount, nextToken));
        }

        @Override
        public ListenableFuture<PrestoThriftSplitBatch> getIndexSplits(
                PrestoThriftSchemaTableName schemaTableName,
                List<String> indexColumnNames,
                List<String> outputColumnNames,
                PrestoThriftPageResult keys,
                PrestoThriftTupleDomain outputConstraint,
                int maxSplitCount,
                PrestoThriftNullableToken nextToken)
                throws PrestoThriftServiceException
        {
            return retry.runAsync("getLookupSplits", () -> getClient().getIndexSplits(schemaTableName, indexColumnNames, outputColumnNames, keys, outputConstraint, maxSplitCount, nextToken));
        }

        @Override
        public ListenableFuture<PrestoThriftPageResult> getRows(PrestoThriftId splitId, List<String> columns, long maxBytes, PrestoThriftNullableToken nextToken)
        {
            return retry.runAsync("getRows", () -> getClient().getRows(splitId, columns, maxBytes, nextToken));
        }

        @Override
        public void close()
        {
            if (client == null) {
                return;
            }
            try {
                client.close();
            }
            catch (Exception e) {
                log.warn(e, "Error closing client");
            }
            client = null;
        }
    }
}
