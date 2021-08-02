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
package com.facebook.presto.spark;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spark.classloader_interface.PrestoSparkStorageHandle;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.storage.TempDataOperationContext;
import com.facebook.presto.spi.storage.TempStorage;
import com.facebook.presto.spi.storage.TempStorageHandle;
import io.airlift.units.DataSize;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeoutException;

import static com.facebook.presto.ExceededMemoryLimitException.exceededLocalBroadcastMemoryLimit;
import static com.facebook.presto.spark.SparkErrorCode.STORAGE_ERROR;
import static com.facebook.presto.spark.util.PrestoSparkUtils.computeNextTimeout;
import static io.airlift.units.DataSize.succinctBytes;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.stream.Collectors.toList;

public class PrestoSparkStorageBasedBroadcastDependency
        implements PrestoSparkBroadcastDependency<PrestoSparkStorageHandle>
{
    private static final Logger log = Logger.get(PrestoSparkStorageBasedBroadcastDependency.class);

    private RddAndMore<PrestoSparkStorageHandle> broadcastDependency;
    private final DataSize maxBroadcastSize;
    private final long queryCompletionDeadline;
    private final TempStorage tempStorage;
    private final TempDataOperationContext tempDataOperationContext;
    private final Set<PrestoSparkServiceWaitTimeMetrics> waitTimeMetrics;

    private Broadcast<List<PrestoSparkStorageHandle>> broadcastVariable;

    public PrestoSparkStorageBasedBroadcastDependency(RddAndMore<PrestoSparkStorageHandle> broadcastDependency, DataSize maxBroadcastSize, long queryCompletionDeadline, TempStorage tempStorage, TempDataOperationContext tempDataOperationContext, Set<PrestoSparkServiceWaitTimeMetrics> waitTimeMetrics)
    {
        this.broadcastDependency = requireNonNull(broadcastDependency, "broadcastDependency cannot be null");
        this.maxBroadcastSize = requireNonNull(maxBroadcastSize, "maxBroadcastSize cannot be null");
        this.queryCompletionDeadline = queryCompletionDeadline;
        this.tempStorage = requireNonNull(tempStorage, "tempStorage cannot be null");
        this.tempDataOperationContext = requireNonNull(tempDataOperationContext, "tempDataOperationContext cannot be null");
        this.waitTimeMetrics = requireNonNull(waitTimeMetrics, "waitTimeMetrics cannot be null");
    }

    @Override
    public Broadcast<List<PrestoSparkStorageHandle>> executeBroadcast(JavaSparkContext sparkContext)
            throws SparkException, TimeoutException
    {
        List<PrestoSparkStorageHandle> broadcastValue = broadcastDependency.collectAndDestroyDependenciesWithTimeout(computeNextTimeout(queryCompletionDeadline), MILLISECONDS, waitTimeMetrics).stream()
                .map(Tuple2::_2)
                .collect(toList());

        // release memory retained by the RDD (splits and dependencies)
        broadcastDependency = null;

        long compressedBroadcastSizeInBytes = broadcastValue.stream()
                .mapToLong(metadata -> metadata.getCompressedSizeInBytes())
                .sum();
        long uncompressedBroadcastSizeInBytes = broadcastValue.stream()
                .mapToLong(metadata -> metadata.getUncompressedSizeInBytes())
                .sum();

        log.info("Got back %d pages. compressedBroadcastSizeInBytes: %d; uncompressedBroadcastSizeInBytes: %d",
                broadcastValue.size(),
                compressedBroadcastSizeInBytes,
                uncompressedBroadcastSizeInBytes);

        long maxBroadcastSizeInBytes = maxBroadcastSize.toBytes();

        if (compressedBroadcastSizeInBytes > maxBroadcastSizeInBytes) {
            throw exceededLocalBroadcastMemoryLimit(maxBroadcastSize, format("Compressed broadcast size: %s", succinctBytes(compressedBroadcastSizeInBytes)));
        }

        if (uncompressedBroadcastSizeInBytes > maxBroadcastSizeInBytes) {
            throw exceededLocalBroadcastMemoryLimit(maxBroadcastSize, format("Uncompressed broadcast size: %s", succinctBytes(uncompressedBroadcastSizeInBytes)));
        }

        broadcastVariable = sparkContext.broadcast(broadcastValue);
        return broadcastVariable;
    }

    @Override
    public void destroy()
    {
        if (broadcastVariable == null) {
            return;
        }

        try {
            // Delete the files
            for (PrestoSparkStorageHandle diskPage : broadcastVariable.getValue()) {
                TempStorageHandle storageHandle = tempStorage.deserialize(diskPage.getSerializedStorageHandle());
                tempStorage.remove(tempDataOperationContext, storageHandle);
                log.info("Deleted broadcast spill file: " + storageHandle.toString());
            }
        }
        catch (IOException e) {
            throw new PrestoException(STORAGE_ERROR, "Unable to delete broadcast spill file", e);
        }

        // Destroy broadcast variable
        broadcastVariable.destroy();
    }
}
