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

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hive.cache.HiveCachingHdfsConfiguration.CachingJobConf;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.util.HiveFileIterator;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.base.Splitter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieEngineContext;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.Option;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.hive.HiveFileInfo.createHiveFileInfo;
import static com.facebook.presto.hive.HiveSessionProperties.getHudiTablesUseMergedView;
import static com.facebook.presto.hive.HiveSessionProperties.isHudiMetadataEnabled;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT;
import static org.apache.hudi.common.model.HoodieTableType.MERGE_ON_READ;

public class HudiDirectoryLister
        implements DirectoryLister
{
    private static final Logger log = Logger.get(HudiDirectoryLister.class);
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();

    private final HoodieTableFileSystemView fileSystemView;
    private final HoodieTableMetaClient metaClient;
    private final boolean metadataEnabled;
    private final String latestInstant;
    private final boolean shouldUseMergedView;

    public HudiDirectoryLister(Configuration conf, ConnectorSession session, Table table)
    {
        log.info("Using Hudi Directory Lister.");
        this.metadataEnabled = isHudiMetadataEnabled(session);
        this.shouldUseMergedView = SPLITTER.splitToList(getHudiTablesUseMergedView(session)).contains(table.getSchemaTableName().toString());
        Configuration actualConfig = ((CachingJobConf) conf).getConfig();
        /*
        WrapperJobConf acts as a wrapper on top of the actual Configuration object. If `hive.copy-on-first-write-configuration-enabled`
        is set to true, the wrapped object is instance of CopyOnFirstWriteConfiguration.
         */
        if (actualConfig instanceof CopyOnFirstWriteConfiguration) {
            actualConfig = ((CopyOnFirstWriteConfiguration) actualConfig).getConfig();
        }
        this.metaClient = HoodieTableMetaClient.builder()
                .setConf(actualConfig)
                .setBasePath(table.getStorage().getLocation())
                .build();
        this.latestInstant = metaClient.getActiveTimeline()
                .getCommitsAndCompactionTimeline()
                .filterCompletedInstants()
                .filter(instant -> MERGE_ON_READ.equals(metaClient.getTableType()) && instant.getAction().equals(HoodieTimeline.COMPACTION_ACTION))
                .lastInstant()
                .map(HoodieInstant::getTimestamp).orElseGet(() -> metaClient.getActiveTimeline()
                        .getCommitsTimeline()
                        .filterCompletedInstants()
                        .lastInstant()
                        .map(HoodieInstant::getTimestamp).orElseThrow(() -> new RuntimeException("No active instant found")));
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(actualConfig);
        HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder()
                .enable(metadataEnabled)
                .build();
        this.fileSystemView = FileSystemViewManager.createInMemoryFileSystemView(engineContext, metaClient, metadataConfig);
    }

    public HoodieTableMetaClient getMetaClient()
    {
        return metaClient;
    }

    @Override
    public Iterator<HiveFileInfo> list(
            ExtendedFileSystem fileSystem,
            Table table,
            Path path,
            Optional<Partition> partition,
            NamenodeStats namenodeStats,
            HiveDirectoryContext hiveDirectoryContext)
    {
        log.debug("Listing path using Hudi directory lister: %s", path.toString());
        return new HiveFileIterator(
                path,
                p -> new HudiFileInfoIterator(
                        fileSystemView,
                        metadataEnabled ? Optional.empty() : Optional.of(fileSystem.listStatus(p)),
                        table.getStorage().getLocation(),
                        p,
                        metaClient.getTableType(),
                        latestInstant,
                        shouldUseMergedView),
                namenodeStats,
                hiveDirectoryContext.getNestedDirectoryPolicy(),
                hiveDirectoryContext.isSkipEmptyFilesEnabled());
    }

    public static class HudiFileInfoIterator
            implements RemoteIterator<HiveFileInfo>
    {
        private final Iterator<HoodieBaseFile> hoodieBaseFileIterator;

        public HudiFileInfoIterator(
                HoodieTableFileSystemView fileSystemView,
                Optional<FileStatus[]> fileStatuses,
                String tablePath,
                Path directory,
                HoodieTableType tableType,
                String latestInstant,
                boolean shouldUseMergedView)
        {
            String partition = FSUtils.getRelativePartitionPath(new Path(tablePath), directory);
            if (fileStatuses.isPresent()) {
                fileSystemView.addFilesToView(fileStatuses.get());
                this.hoodieBaseFileIterator = fileSystemView.fetchLatestBaseFiles(partition).iterator();
            }
            else {
                if (shouldUseMergedView) {
                    Stream<FileSlice> fileSlices = MERGE_ON_READ.equals(tableType) ?
                            fileSystemView.getLatestMergedFileSlicesBeforeOrOn(partition, latestInstant) :
                            fileSystemView.getLatestFileSlicesBeforeOrOn(partition, latestInstant, false);
                    this.hoodieBaseFileIterator = fileSlices.map(FileSlice::getBaseFile).filter(Option::isPresent).map(Option::get).iterator();
                }
                else {
                    this.hoodieBaseFileIterator = fileSystemView.getLatestBaseFiles(partition).iterator();
                }
            }
        }

        @Override
        public boolean hasNext()
        {
            return hoodieBaseFileIterator.hasNext();
        }

        @Override
        public HiveFileInfo next()
                throws IOException
        {
            FileStatus fileStatus = hoodieBaseFileIterator.next().getFileStatus();
            String[] name = {"localhost:" + DFS_DATANODE_DEFAULT_PORT};
            String[] host = {"localhost"};
            LocatedFileStatus hoodieFileStatus = new LocatedFileStatus(fileStatus,
                    new BlockLocation[] {new BlockLocation(name, host, 0L, fileStatus.getLen())});
            return createHiveFileInfo(hoodieFileStatus, Optional.empty());
        }
    }
}
