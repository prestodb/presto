package com.facebook.presto.hudi.query;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.hudi.HudiPartition;
import com.facebook.presto.hudi.HudiTableHandle;
import com.facebook.presto.hudi.HudiTableLayoutHandle;
import com.facebook.presto.hudi.query.index.HudiIndexSupport;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.SchemaTableName;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.engine.HoodieLocalEngineContext;
import org.apache.hudi.common.model.FileSlice;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.view.HoodieTableFileSystemView;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.metadata.HoodieTableMetadata;
import org.apache.hudi.util.Lazy;

import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.hudi.HudiSessionProperties.isHudiMetadataTableEnabled;
import static com.facebook.presto.hudi.query.index.IndexSupportFactory.createIndexSupport;
import static org.apache.hudi.common.table.view.FileSystemViewManager.createInMemoryFileSystemViewWithTimeline;

public class HudiSnapshotDirectoryLister
        implements HudiDirectoryLister
{
    private static final Logger log = Logger.get(HudiSnapshotDirectoryLister.class);
    private final HudiTableHandle tableHandle;
    private final Lazy<HoodieTableFileSystemView> lazyFileSystemView;
    private final Optional<HudiIndexSupport> indexSupportOpt;

    public HudiSnapshotDirectoryLister(
            ConnectorSession session,
            HudiTableLayoutHandle layoutHandle,
            boolean enableMetadataTable,
            Lazy<HoodieTableMetadata> lazyTableMetadata)
    {
        this.tableHandle = layoutHandle.getTableHandle();
        SchemaTableName schemaTableName = tableHandle.getSchemaTableName();
        this.lazyFileSystemView = Lazy.lazily(() -> {
            HoodieTimer timer = HoodieTimer.start();
            HoodieTableMetaClient metaClient = layoutHandle.getTableHandle().getMetaClient();
            HoodieMetadataConfig metadataConfig = HoodieMetadataConfig.newBuilder().enable(isHudiMetadataTableEnabled(session)).build();
            HoodieTimeline timeline = metaClient.getActiveTimeline().getCommitsTimeline().filterCompletedInstants();
            HoodieLocalEngineContext engineContext = new HoodieLocalEngineContext(metaClient.getStorageConf());
            HoodieTableFileSystemView fsView = createInMemoryFileSystemViewWithTimeline(engineContext, metaClient, metadataConfig, timeline);
            log.info("Created file system view of table %s in %s ms", schemaTableName, timer.endTimer());
            return fsView;
        });

        Lazy<HoodieTableMetaClient> lazyMetaClient = Lazy.lazily(tableHandle::getMetaClient);
        this.indexSupportOpt = enableMetadataTable ?
                createIndexSupport(layoutHandle, lazyMetaClient, lazyTableMetadata, layoutHandle.getRegularPredicates(), session) : Optional.empty();
    }

    @Override
    public Stream<FileSlice> listStatus(HudiPartition hudiPartition, boolean useIndex)
    {
        HoodieTimer timer = HoodieTimer.start();
        Stream<FileSlice> slices = lazyFileSystemView.get().getLatestFileSlicesBeforeOrOn(
                hudiPartition.getRelativePartitionPath(),
                tableHandle.getLatestCommitTime(),
                false);

        if (!useIndex) {
            return slices;
        }

        Stream<FileSlice> fileSlices = slices
                .filter(slice -> indexSupportOpt
                        .map(indexSupport -> !indexSupport.shouldSkipFileSlice(slice))
                        .orElse(true));
        log.info("Listed partition [%s] on table %s.%s in %s ms",
                hudiPartition, tableHandle.getSchemaName(), tableHandle.getTableName(), timer.endTimer());
        return fileSlices;
    }

    @Override
    public void close()
    {
        if (!lazyFileSystemView.get().isClosed()) {
            lazyFileSystemView.get().close();
        }
    }
}
