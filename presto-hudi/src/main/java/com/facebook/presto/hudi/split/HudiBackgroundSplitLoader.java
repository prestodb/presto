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

package com.facebook.presto.hudi.split;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.metastore.ExtendedHiveMetastore;
import com.facebook.presto.hive.util.AsyncQueue;
import com.facebook.presto.hudi.HudiTableLayoutHandle;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.PrestoException;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

import static com.facebook.presto.hudi.HudiErrorCode.HUDI_CANNOT_GENERATE_SPLIT;
import static com.facebook.presto.hudi.HudiSessionProperties.getSplitGeneratorParallelism;
import static java.util.Objects.requireNonNull;

/**
 * A runnable to load Hudi splits asynchronously in the background
 */
public class HudiBackgroundSplitLoader
        implements Runnable
{
    private static final Logger log = Logger.get(HudiBackgroundSplitLoader.class);

    private final ConnectorSession session;
    private final ExtendedHiveMetastore metastore;
    private final HudiTableLayoutHandle layout;
    private final HoodieTableFileSystemView fsView;
    private final AsyncQueue<ConnectorSplit> asyncQueue;
    private final List<String> partitions;
    private final String latestInstant;
    private final int splitGeneratorNumThreads;
    private final ExecutorService splitGeneratorExecutorService;

    public HudiBackgroundSplitLoader(
            ConnectorSession session,
            ExtendedHiveMetastore metastore,
            ExecutorService splitGeneratorExecutorService,
            HudiTableLayoutHandle layout,
            HoodieTableFileSystemView fsView,
            AsyncQueue<ConnectorSplit> asyncQueue,
            List<String> partitions,
            String latestInstant)
    {
        this.session = requireNonNull(session, "session is null");
        this.metastore = requireNonNull(metastore, "metastore is null");
        this.layout = requireNonNull(layout, "layout is null");
        this.fsView = requireNonNull(fsView, "fsView is null");
        this.asyncQueue = requireNonNull(asyncQueue, "asyncQueue is null");
        this.partitions = requireNonNull(partitions, "partitions is null");
        this.latestInstant = requireNonNull(latestInstant, "latestInstant is null");

        this.splitGeneratorNumThreads = getSplitGeneratorParallelism(session);
        this.splitGeneratorExecutorService = requireNonNull(splitGeneratorExecutorService, "splitGeneratorExecutorService is null");
    }

    @Override
    public void run()
    {
        HoodieTimer timer = new HoodieTimer().startTimer();
        List<HudiPartitionSplitGenerator> splitGeneratorList = new ArrayList<>();
        List<Future> splitGeneratorFutures = new ArrayList<>();
        ConcurrentLinkedQueue<String> concurrentPartitionQueue = new ConcurrentLinkedQueue<>(partitions);

        // Start a number of partition split generators to generate the splits in parallel
        for (int i = 0; i < splitGeneratorNumThreads; i++) {
            HudiPartitionSplitGenerator generator = new HudiPartitionSplitGenerator(
                    session, metastore, layout, fsView, asyncQueue, concurrentPartitionQueue, latestInstant);
            splitGeneratorList.add(generator);
            splitGeneratorFutures.add(splitGeneratorExecutorService.submit(generator));
        }

        // Wait for all split generators to finish
        for (Future future : splitGeneratorFutures) {
            try {
                future.get();
            }
            catch (InterruptedException | ExecutionException e) {
                throw new PrestoException(HUDI_CANNOT_GENERATE_SPLIT, "Error generating Hudi split", e);
            }
        }
        asyncQueue.finish();
        log.debug("Finished getting all splits in %d ms", timer.endTimer());
    }
}
