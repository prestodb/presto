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
package com.facebook.presto.delta;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.facebook.presto.spi.connector.ConnectorSplitManager;
import com.facebook.presto.spi.connector.ConnectorTransactionHandle;
import com.google.common.collect.ImmutableList;
import io.delta.kernel.data.Row;
import io.delta.kernel.internal.InternalScanFileUtils;
import io.delta.kernel.utils.CloseableIterator;
import io.delta.kernel.utils.FileStatus;
import jakarta.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.hive.HiveCommonSessionProperties.getNodeSelectionStrategy;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.stream.Collectors.toMap;

public class DeltaSplitManager
        implements ConnectorSplitManager
{
    private final String connectorId;
    private final DeltaConfig deltaConfig;
    private final DeltaClient deltaClient;
    private final TypeManager typeManager;

    @Inject
    public DeltaSplitManager(DeltaConnectorId connectorId, DeltaConfig deltaConfig, DeltaClient deltaClient, TypeManager typeManager)
    {
        this.connectorId = requireNonNull(connectorId, "connectorId is null").toString();
        this.deltaConfig = requireNonNull(deltaConfig, "deltaConfig is null");
        this.deltaClient = requireNonNull(deltaClient, "deltaClient is null");
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public ConnectorSplitSource getSplits(
            ConnectorTransactionHandle handle,
            ConnectorSession session,
            ConnectorTableLayoutHandle layout,
            SplitSchedulingContext splitSchedulingContext)
    {
        return new DeltaSplitSource(session, ((DeltaTableLayoutHandle) layout));
    }

    private class DeltaSplitSource
            implements ConnectorSplitSource
    {
        private final DeltaTable deltaTable;
        private final CloseableIterator<Row> rowIterator;
        private final int maxBatchSize;
        private final ConnectorSession session;

        DeltaSplitSource(ConnectorSession session, DeltaTableLayoutHandle deltaTableHandle)
        {
            this.session = requireNonNull(session, "session is null");
            this.deltaTable = deltaTableHandle.getTable().getDeltaTable();
            this.rowIterator = DeltaExpressionUtils.iterateWithPartitionPruning(
                    deltaClient.listFiles(session, deltaTable),
                    deltaTableHandle.getPredicate(),
                    typeManager);
            this.maxBatchSize = deltaConfig.getMaxSplitsBatchSize();
        }

        @Override
        public CompletableFuture<ConnectorSplitBatch> getNextBatch(ConnectorPartitionHandle partitionHandle, int maxSize)
        {
            ImmutableList.Builder<ConnectorSplit> splitBuilder = ImmutableList.builder();
            long currentSplitCount = 0;
            while (rowIterator.hasNext() && currentSplitCount < maxSize && currentSplitCount < maxBatchSize) {
                Row row = rowIterator.next();
                FileStatus addFileStatus = InternalScanFileUtils.getAddFileStatus(row);
                splitBuilder.add(new DeltaSplit(
                        connectorId,
                        deltaTable.getSchemaName(),
                        deltaTable.getTableName(),
                        addFileStatus.getPath(),
                        0, /* start */
                        addFileStatus.getSize() /* split length - default is read the entire file in one split */,
                        addFileStatus.getSize(),
                        removeNullPartitionValues(InternalScanFileUtils.getPartitionValues(row)),
                        getNodeSelectionStrategy(session)));
                currentSplitCount++;
            }

            return completedFuture(new ConnectorSplitBatch(splitBuilder.build(), !rowIterator.hasNext()));
        }

        @Override
        public void close()
        {
            try {
                rowIterator.close();
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public boolean isFinished()
        {
            return !rowIterator.hasNext();
        }
    }

    /**
     * Utility method to remove the null value partition values.
     * These null values cause problems later when used with Guava Immutable map structures.
     */
    private static Map<String, String> removeNullPartitionValues(Map<String, String> partitionValues)
    {
        return partitionValues.entrySet().stream()
                .filter(entry -> entry.getValue() != null)
                .collect(toMap(Entry::getKey, Entry::getValue));
    }
}
