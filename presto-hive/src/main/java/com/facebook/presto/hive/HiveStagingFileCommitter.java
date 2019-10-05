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

import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.facebook.presto.hive.PartitionUpdate.FileWriteInfo;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getFileSystem;
import static com.facebook.presto.hive.metastore.MetastoreUtil.renameFile;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.util.concurrent.Futures.whenAllSucceed;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.util.Objects.requireNonNull;

public class HiveStagingFileCommitter
        implements StagingFileCommitter
{
    private final HdfsEnvironment hdfsEnvironment;
    private final ListeningExecutorService fileRenameExecutor;

    @Inject
    public HiveStagingFileCommitter(
            HdfsEnvironment hdfsEnvironment,
            @ForFileRename ListeningExecutorService fileRenameExecutor)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.fileRenameExecutor = requireNonNull(fileRenameExecutor, "fileRenameExecutor is null");
    }

    @Override
    public void commitFiles(ConnectorSession session, String schemaName, String tableName, List<PartitionUpdate> partitionUpdates)
    {
        HdfsContext context = new HdfsContext(session, schemaName, tableName);
        List<ListenableFuture<?>> commitFutures = new ArrayList<>();

        for (PartitionUpdate partitionUpdate : partitionUpdates) {
            Path path = partitionUpdate.getWritePath();
            FileSystem fileSystem = getFileSystem(hdfsEnvironment, context, path);
            for (FileWriteInfo fileWriteInfo : partitionUpdate.getFileWriteInfos()) {
                checkState(!fileWriteInfo.getWriteFileName().equals(fileWriteInfo.getTargetFileName()));
                Path source = new Path(path, fileWriteInfo.getWriteFileName());
                Path target = new Path(path, fileWriteInfo.getTargetFileName());
                commitFutures.add(fileRenameExecutor.submit(() -> renameFile(fileSystem, source, target)));
            }
        }

        ListenableFuture<?> listenableFutureAggregate = whenAllSucceed(commitFutures).call(() -> null, directExecutor());
        try {
            getFutureValue(listenableFutureAggregate, PrestoException.class);
        }
        catch (RuntimeException e) {
            listenableFutureAggregate.cancel(true);
            throw e;
        }
    }
}
