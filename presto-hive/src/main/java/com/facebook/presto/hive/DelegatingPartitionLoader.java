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
package com.facebook.presto.hive;

import com.facebook.presto.common.predicate.Domain;
import com.facebook.presto.hive.StoragePartitionLoader.BucketSplitInfo;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.IOException;
import java.util.Deque;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveManifestUtils.MANIFEST_VERSION;
import static com.facebook.presto.hive.HiveSessionProperties.isPreferManifestsToListFiles;
import static java.util.Objects.requireNonNull;

public class DelegatingPartitionLoader
        extends PartitionLoader
{
    private final ConnectorSession session;
    private final PartitionLoader storagePartitionLoader;
    private final PartitionLoader manifestPartitionLoader;

    public DelegatingPartitionLoader(
            Table table,
            Optional<Domain> pathDomain,
            Optional<BucketSplitInfo> tableBucketInfo,
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            NamenodeStats namenodeStats,
            DirectoryLister directoryLister,
            Deque<Iterator<InternalHiveSplit>> fileIterators,
            boolean recursiveDirWalkerEnabled,
            boolean schedulerUsesHostAddresses,
            boolean partialAggregationsPushedDown)
    {
        this.session = requireNonNull(session, "session is null");
        this.storagePartitionLoader = new StoragePartitionLoader(
                table,
                pathDomain,
                tableBucketInfo,
                session,
                hdfsEnvironment,
                namenodeStats,
                directoryLister,
                fileIterators,
                recursiveDirWalkerEnabled,
                schedulerUsesHostAddresses,
                partialAggregationsPushedDown);
        this.manifestPartitionLoader = new ManifestPartitionLoader(table, pathDomain, session, hdfsEnvironment, namenodeStats, directoryLister, recursiveDirWalkerEnabled, schedulerUsesHostAddresses);
    }

    @Override
    public ListenableFuture<?> loadPartition(HivePartitionMetadata partition, HiveSplitSource hiveSplitSource, boolean stopped)
            throws IOException
    {
        if (isListFilesLoadedPartition(session, partition.getPartition())) {
            // Partition has list of file names and sizes. We can avoid the listFiles() call to underlying storage.
            return manifestPartitionLoader.loadPartition(partition, hiveSplitSource, stopped);
        }

        return storagePartitionLoader.loadPartition(partition, hiveSplitSource, stopped);
    }

    private static boolean isListFilesLoadedPartition(ConnectorSession session, Optional<Partition> partition)
    {
        if (partition.isPresent() && isPreferManifestsToListFiles(session)) {
            Map<String, String> parameters = partition.get().getParameters();
            return parameters.containsKey(MANIFEST_VERSION);
        }

        return false;
    }
}
