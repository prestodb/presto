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
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.util.HiveFileIterator;
import com.facebook.presto.spi.ConnectorSession;
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
import org.apache.hudi.common.model.HoodieBaseFile;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.view.FileSystemViewManager;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;

import java.io.IOException;
import java.util.Iterator;
import java.util.Optional;

import static com.facebook.presto.hive.HiveFileInfo.createHiveFileInfo;
import static com.facebook.presto.hive.HiveSessionProperties.isHudiMetadataEnabled;
import static org.apache.hadoop.hdfs.DFSConfigKeys.DFS_DATANODE_DEFAULT_PORT;

public class HudiDirectoryLister
        implements DirectoryLister
{
    private static final Logger log = Logger.get(HudiDirectoryLister.class);

    private final HoodieTableFileSystemView fileSystemView;
    private final HoodieTableMetaClient metaClient;
    private final boolean metadataEnabled;

    public HudiDirectoryLister(Configuration conf, ConnectorSession session, Table table)
    {
        log.info("Using Hudi Directory Lister.");
        this.metadataEnabled = isHudiMetadataEnabled(session);
        this.metaClient = HoodieTableMetaClient.builder()
                .setConf(conf)
                .setBasePath(table.getStorage().getLocation())
                .build();
        HoodieEngineContext engineContext = new HoodieLocalEngineContext(conf);
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
                        p),
                namenodeStats,
                hiveDirectoryContext.getNestedDirectoryPolicy());
    }

    public static class HudiFileInfoIterator
            implements RemoteIterator<HiveFileInfo>
    {
        private final Iterator<HoodieBaseFile> hoodieBaseFileIterator;

        public HudiFileInfoIterator(
                HoodieTableFileSystemView fileSystemView,
                Optional<FileStatus[]> fileStatuses,
                String tablePath,
                Path directory)
        {
            String partition = FSUtils.getRelativePartitionPath(new Path(tablePath), directory);
            if (fileStatuses.isPresent()) {
                fileSystemView.addFilesToView(fileStatuses.get());
                this.hoodieBaseFileIterator = fileSystemView.fetchLatestBaseFiles(partition).iterator();
            }
            else {
                this.hoodieBaseFileIterator = fileSystemView.getLatestBaseFiles(partition).iterator();
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
            String[] name = new String[] {"localhost:" + DFS_DATANODE_DEFAULT_PORT};
            String[] host = new String[] {"localhost"};
            LocatedFileStatus hoodieFileStatus = new LocatedFileStatus(fileStatus,
                    new BlockLocation[] {new BlockLocation(name, host, 0L, fileStatus.getLen())});
            return createHiveFileInfo(hoodieFileStatus, Optional.empty());
        }
    }
}
