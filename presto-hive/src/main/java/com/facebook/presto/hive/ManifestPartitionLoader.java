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
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.hive.util.InternalHiveSplitFactory;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.InputFormat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.MALFORMED_HIVE_FILE_STATISTICS;
import static com.facebook.presto.hive.HiveManifestUtils.FILE_NAMES;
import static com.facebook.presto.hive.HiveManifestUtils.FILE_SIZES;
import static com.facebook.presto.hive.HiveManifestUtils.MANIFEST_VERSION;
import static com.facebook.presto.hive.HiveManifestUtils.VERSION_1;
import static com.facebook.presto.hive.HiveManifestUtils.decompressFileNames;
import static com.facebook.presto.hive.HiveManifestUtils.decompressFileSizes;
import static com.facebook.presto.hive.HiveSessionProperties.getMaxInitialSplitSize;
import static com.facebook.presto.hive.HiveSessionProperties.getMaxSplitSize;
import static com.facebook.presto.hive.HiveSessionProperties.getNodeSelectionStrategy;
import static com.facebook.presto.hive.HiveSessionProperties.isManifestVerificationEnabled;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.facebook.presto.hive.NestedDirectoryPolicy.IGNORED;
import static com.facebook.presto.hive.NestedDirectoryPolicy.RECURSE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.getPartitionLocation;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class ManifestPartitionLoader
        extends PartitionLoader
{
    // The following constants are referred from FileSystem.getFileBlockLocations in Hadoop
    private static final String[] BLOCK_LOCATION_NAMES = new String[] {"localhost:50010"};
    private static final String[] BLOCK_LOCATION_HOSTS = new String[] {"localhost"};

    private final Table table;
    private final Optional<Domain> pathDomain;
    private final ConnectorSession session;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final NamenodeStats namenodeStats;
    private final DirectoryLister directoryLister;
    private final boolean recursiveDirWalkerEnabled;
    private final boolean schedulerUsesHostAddresses;

    public ManifestPartitionLoader(
            Table table,
            Optional<Domain> pathDomain,
            ConnectorSession session,
            HdfsEnvironment hdfsEnvironment,
            NamenodeStats namenodeStats,
            DirectoryLister directoryLister,
            boolean recursiveDirWalkerEnabled,
            boolean schedulerUsesHostAddresses)
    {
        this.table = requireNonNull(table, "table is null");
        this.pathDomain = requireNonNull(pathDomain, "pathDomain is null");
        this.session = requireNonNull(session, "session is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = new HdfsContext(session, table.getDatabaseName(), table.getTableName(), table.getStorage().getLocation(), false);
        this.namenodeStats = requireNonNull(namenodeStats, "namenodeStats is null");
        this.directoryLister = requireNonNull(directoryLister, "directoryLister is null");
        this.recursiveDirWalkerEnabled = recursiveDirWalkerEnabled;
        this.schedulerUsesHostAddresses = schedulerUsesHostAddresses;
    }

    public ListenableFuture<?> loadPartition(HivePartitionMetadata partition, HiveSplitSource hiveSplitSource, boolean stopped)
            throws IOException
    {
        Path path = new Path(getPartitionLocation(table, partition.getPartition()));
        Map<String, String> parameters = partition.getPartition().get().getParameters();

        // TODO: Add support for more manifest versions
        // Verify the manifest version
        verify(VERSION_1.equals(parameters.get(MANIFEST_VERSION)), "Manifest version is not equal to %s", VERSION_1);

        List<String> fileNames = decompressFileNames(parameters.get(FILE_NAMES));
        List<Long> fileSizes = decompressFileSizes(parameters.get(FILE_SIZES));

        // Verify that the count of fileNames and fileSizes are same
        verify(fileNames.size() == fileSizes.size(), "List of fileNames and fileSizes differ in length");

        if (isManifestVerificationEnabled(session)) {
            // Verify that the file names and sizes in manifest are the same as listed by directory lister
            validateManifest(partition, path, fileNames, fileSizes);
        }

        ImmutableList.Builder<HiveFileInfo> fileListBuilder = ImmutableList.builder();
        for (int i = 0; i < fileNames.size(); i++) {
            Path filePath = new Path(path, fileNames.get(i));
            FileStatus fileStatus = new FileStatus(fileSizes.get(i), false, 1, getMaxSplitSize(session).toBytes(), 0, filePath);
            try {
                BlockLocation[] locations = new BlockLocation[] {new BlockLocation(BLOCK_LOCATION_NAMES, BLOCK_LOCATION_HOSTS, 0, fileSizes.get(i))};

                // It is safe to set extraFileContext as empty because downstream code always checks if its present before proceeding.
                fileListBuilder.add(HiveFileInfo.createHiveFileInfo(new LocatedFileStatus(fileStatus, locations), Optional.empty()));
            }
            catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        InternalHiveSplitFactory splitFactory = createInternalHiveSplitFactory(table, partition, session, pathDomain, hdfsEnvironment, hdfsContext, schedulerUsesHostAddresses);

        return hiveSplitSource.addToQueue(fileListBuilder.build().stream()
                .map(status -> splitFactory.createInternalHiveSplit(status, true))
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(toImmutableList()));
    }

    private InternalHiveSplitFactory createInternalHiveSplitFactory(
            Table table,
            HivePartitionMetadata partition,
            ConnectorSession session,
            Optional<Domain> pathDomain,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            boolean schedulerUsesHostAddresses)
            throws IOException
    {
        String partitionName = partition.getHivePartition().getPartitionId();
        Storage storage = partition.getPartition().map(Partition::getStorage).orElse(table.getStorage());
        String inputFormatName = storage.getStorageFormat().getInputFormat();
        int partitionDataColumnCount = partition.getPartition()
                .map(p -> p.getColumns().size())
                .orElse(table.getDataColumns().size());
        List<HivePartitionKey> partitionKeys = getPartitionKeys(table, partition.getPartition(), partitionName);
        Path path = new Path(getPartitionLocation(table, partition.getPartition()));
        Configuration configuration = hdfsEnvironment.getConfiguration(hdfsContext, path);
        InputFormat<?, ?> inputFormat = getInputFormat(configuration, inputFormatName, false);
        ExtendedFileSystem fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, path);

        return new InternalHiveSplitFactory(
                fileSystem,
                inputFormat,
                pathDomain,
                getNodeSelectionStrategy(session),
                getMaxInitialSplitSize(session),
                false,
                new HiveSplitPartitionInfo(
                        storage,
                        path.toUri(),
                        partitionKeys,
                        partitionName,
                        partitionDataColumnCount,
                        partition.getTableToPartitionMapping(),
                        Optional.empty(),
                        partition.getRedundantColumnDomains()),
                schedulerUsesHostAddresses,
                partition.getEncryptionInformation());
    }

    private void validateManifest(HivePartitionMetadata partition, Path path, List<String> manifestFileNames, List<Long> manifestFileSizes)
            throws IOException
    {
        ExtendedFileSystem fileSystem = hdfsEnvironment.getFileSystem(hdfsContext, path);
        HiveDirectoryContext hiveDirectoryContext = new HiveDirectoryContext(recursiveDirWalkerEnabled ? RECURSE : IGNORED, false);

        Iterator<HiveFileInfo> fileInfoIterator = directoryLister.list(fileSystem, table, path, namenodeStats, ignore -> true, hiveDirectoryContext);
        int fileCount = 0;
        while (fileInfoIterator.hasNext()) {
            HiveFileInfo fileInfo = fileInfoIterator.next();
            String fileName = fileInfo.getPath().getName();
            if (!manifestFileNames.contains(fileName)) {
                throw new PrestoException(
                        MALFORMED_HIVE_FILE_STATISTICS,
                        format("Filename = %s not stored in manifest. Partition = %s, TableName = %s",
                                fileName,
                                partition.getHivePartition().getPartitionId(),
                                table.getTableName()));
            }

            int index = manifestFileNames.indexOf(fileName);
            if (!manifestFileSizes.get(index).equals(fileInfo.getLength())) {
                throw new PrestoException(
                        MALFORMED_HIVE_FILE_STATISTICS,
                        format("FilesizeFromManifest = %s is not equal to FilesizeFromStorage = %s. File = %s, Partition = %s, TableName = %s",
                                manifestFileSizes.get(index),
                                fileInfo.getLength(),
                                fileName,
                                partition.getHivePartition().getPartitionId(),
                                table.getTableName()));
            }

            fileCount++;
        }

        if (fileCount != manifestFileNames.size()) {
            throw new PrestoException(
                    MALFORMED_HIVE_FILE_STATISTICS,
                    format("Number of files in Manifest = %s is not equal to Number of files in storage = %s. Partition = %s, TableName = %s",
                            manifestFileNames.size(),
                            fileCount,
                            partition.getHivePartition().getPartitionId(),
                            table.getTableName()));
        }
    }
}
