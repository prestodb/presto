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

package com.facebook.presto.hudi;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.hudi.query.HudiDirectoryLister;
import com.facebook.presto.hudi.query.HudiSnapshotDirectoryLister;
import com.facebook.presto.hudi.split.HudiBackgroundSplitLoader;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.ConnectorSplitSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.connector.ConnectorPartitionHandle;
import com.google.common.util.concurrent.Futures;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.metadata.HoodieBackedTableMetadata;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.util.Lazy;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.airlift.concurrent.MoreFutures.toCompletableFuture;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hudi.HudiSessionProperties.isHudiMetadataTableEnabled;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class HudiSplitSource
        implements ConnectorSplitSource
{
    private static final Logger log = Logger.get(HudiSplitManager.class);

    private final AsyncQueue<ConnectorSplit> queue;
    private final HudiBackgroundSplitLoader splitLoader;
    private final ScheduledFuture splitLoaderFuture;
    private final AtomicReference<PrestoException> prestoExceptionReference = new AtomicReference<>();

    public HudiSplitSource(
            ConnectorSession session,
            HudiTableLayoutHandle layout,
            Lazy<Map<String, Partition>> lazyPartitionMap,
            ExecutorService asyncQueueExecutor,
            ScheduledExecutorService splitLoaderExecutorService,
            ExecutorService splitGeneratorExecutorService,
            int maxOutstandingSplits)
    {
        boolean enableMetadataTable = isHudiMetadataTableEnabled(session);
        this.queue = new AsyncQueue<>(maxOutstandingSplits, asyncQueueExecutor);

        SchemaTableName schemaTableName = layout.getTableHandle().getSchemaTableName();
        Lazy<HoodieTableMetadata> lazyTableMetadata = Lazy.lazily(() -> {
            HoodieTimer timer = HoodieTimer.start();
            HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                    .enable(enableMetadataTable)
                    .build();
            HoodieTableMetaClient metaClient = layout.getTableHandle().getMetaClient();
            HoodieEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorage().getConf());

            HoodieTableMetadata tableMetadata = new HoodieBackedTableMetadata(
                    engineContext,
                    layout.getTableHandle().getMetaClient().getStorage(), metadataConfig, metaClient.getBasePath().toString(), true);
            log.info("Loaded table metadata for table: %s in %s ms", schemaTableName, timer.endTimer());
            return tableMetadata;
        });

        HudiDirectoryLister hudiDirectoryLister = new HudiSnapshotDirectoryLister(
                session,
                layout,
                enableMetadataTable,
                lazyTableMetadata);

        this.splitLoader = new HudiBackgroundSplitLoader(
                session,
                splitGeneratorExecutorService,
                layout,
                hudiDirectoryLister,
                queue,
                lazyPartitionMap,
                enableMetadataTable,
                lazyTableMetadata,
                throwable -> {
                    prestoExceptionReference.compareAndSet(null, new PrestoException(HUDI_CANNOT_OPEN_SPLIT,
                            "Failed to generate splits for " + schemaTableName, throwable));
                    queue.finish();
                });
        this.splitLoaderFuture = splitLoaderExecutorService.schedule(
                this.splitLoader, 0, TimeUnit.MILLISECONDS);
    }

    @Override
    public CompletableFuture<ConnectorSplitBatch> getNextBatch(
            ConnectorPartitionHandle partitionHandle,
            int maxSize)
    {
        boolean noMoreSplits = isFinished();

        return toCompletableFuture(Futures.transform(
                queue.getBatchAsync(maxSize),
                splits -> new ConnectorSplitBatch(splits, noMoreSplits),
                directExecutor()));
    }

    @Override
    public void close()
    {
        queue.finish();
    }

    @Override
    public boolean isFinished()
    {
        return splitLoaderFuture.isDone() && queue.isFinished();
    }
}
