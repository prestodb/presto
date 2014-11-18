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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.orc.FileOrcDataSource;
import com.facebook.presto.orc.OrcDataSource;
import com.facebook.presto.orc.OrcPredicate;
import com.facebook.presto.orc.OrcReader;
import com.facebook.presto.orc.OrcRecordReader;
import com.facebook.presto.orc.TupleDomainOrcPredicate;
import com.facebook.presto.orc.TupleDomainOrcPredicate.ColumnReference;
import com.facebook.presto.orc.metadata.OrcMetadataReader;
import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.BYTE;
import static io.airlift.units.Duration.nanosSince;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.StandardCopyOption.ATOMIC_MOVE;
import static java.util.Locale.ENGLISH;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.joda.time.DateTimeZone.UTC;

public class OrcStorageManager
        implements StorageManager
{
    private static final Logger log = Logger.get(OrcStorageManager.class);

    private final File baseStorageDir;
    private final File baseStagingDir;
    private final Optional<File> baseBackupDir;
    private final DataSize orcMaxMergeDistance;

    @Inject
    public OrcStorageManager(StorageManagerConfig config)
    {
        this(config.getDataDirectory(), Optional.fromNullable(config.getBackupDirectory()), config.getOrcMaxMergeDistance());
    }

    public OrcStorageManager(File dataDirectory, Optional<File> backupDirectory, DataSize orcMaxMergeDistance)
    {
        File baseDataDir = checkNotNull(dataDirectory, "dataDirectory is null");
        this.baseStorageDir = new File(baseDataDir, "storage");
        this.baseStagingDir = new File(baseDataDir, "staging");
        this.baseBackupDir = checkNotNull(backupDirectory, "backupDirectory is null");
        this.orcMaxMergeDistance = checkNotNull(orcMaxMergeDistance, "orcMaxMergeDistance is null");
    }

    @PostConstruct
    public void start()
            throws IOException
    {
        deleteDirectory(baseStagingDir);
        createParents(baseStagingDir);
        createParents(baseStorageDir);

        if (baseBackupDir.isPresent()) {
            createParents(baseBackupDir.get());
        }
    }

    @PreDestroy
    public void stop()
            throws IOException
    {
        deleteDirectory(baseStagingDir);
    }

    @Override
    public ConnectorPageSource getPageSource(UUID shardUuid, List<Long> columnIds, List<Type> columnTypes, TupleDomain<RaptorColumnHandle> effectivePredicate)
    {
        OrcDataSource dataSource = openShard(shardUuid);

        try {
            OrcReader reader = new OrcReader(dataSource, new OrcMetadataReader());

            Map<Long, Integer> indexMap = columnIdIndex(reader.getColumnNames());
            ImmutableSet.Builder<Integer> includedColumns = ImmutableSet.builder();
            ImmutableList.Builder<Integer> columnIndexes = ImmutableList.builder();
            for (long columnId : columnIds) {
                Integer index = indexMap.get(columnId);
                if (index == null) {
                    columnIndexes.add(-1);
                }
                else {
                    columnIndexes.add(index);
                    includedColumns.add(index);
                }
            }

            OrcPredicate predicate = getPredicate(effectivePredicate, indexMap);

            OrcRecordReader recordReader = reader.createRecordReader(
                    includedColumns.build(),
                    predicate,
                    0,
                    dataSource.getSize(),
                    UTC);

            return new OrcPageSource(recordReader, dataSource, columnIds, columnTypes, columnIndexes.build());
        }
        catch (IOException | RuntimeException e) {
            try {
                dataSource.close();
            }
            catch (IOException ex) {
                e.addSuppressed(ex);
            }
            throw new PrestoException(RAPTOR_ERROR, "Failed to create page source", e);
        }
    }

    @SuppressWarnings("resource")
    @Override
    public OutputHandle createOutputHandle(List<Long> columnIds, List<Type> columnTypes, Optional<Long> sampleWeightColumnId)
    {
        List<StorageType> storageTypes = toStorageTypes(columnTypes);

        UUID shardUuid = UUID.randomUUID();
        File stagingFile = getStagingFile(shardUuid);
        createParents(stagingFile);

        RowSink rowSink = new OrcRowSink(columnIds, storageTypes, sampleWeightColumnId, stagingFile);

        return new OutputHandle(shardUuid, rowSink);
    }

    @Override
    public void commit(OutputHandle outputHandle)
    {
        outputHandle.getRowSink().close();

        File stagingFile = getStagingFile(outputHandle.getShardUuid());
        File storageFile = getStorageFile(outputHandle.getShardUuid());

        createParents(storageFile);

        try {
            Files.move(stagingFile.toPath(), storageFile.toPath(), ATOMIC_MOVE);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to move shard file", e);
        }

        if (baseBackupDir.isPresent()) {
            File backupFile = getBackupFile(outputHandle.getShardUuid());
            createParents(backupFile);
            try {
                Files.copy(storageFile.toPath(), backupFile.toPath());
            }
            catch (IOException e) {
                throw new PrestoException(RAPTOR_ERROR, "Failed to create backup shard file", e);
            }
        }
    }

    @Override
    public boolean isBackupAvailable()
    {
        return baseBackupDir.isPresent();
    }

    @VisibleForTesting
    File getStorageFile(UUID shardUuid)
    {
        return getFileSystemPath(baseStorageDir, shardUuid);
    }

    @VisibleForTesting
    File getStagingFile(UUID shardUuid)
    {
        String name = getFileSystemPath(new File("/"), shardUuid).getName();
        return new File(baseStagingDir, name);
    }

    @VisibleForTesting
    File getBackupFile(UUID shardUuid)
    {
        checkState(baseBackupDir.isPresent(), "backup directory not set");
        return getFileSystemPath(baseBackupDir.get(), shardUuid);
    }

    @VisibleForTesting
    OrcDataSource openShard(UUID shardUuid)
    {
        File file = getStorageFile(shardUuid).getAbsoluteFile();

        if (!file.exists() && baseBackupDir.isPresent()) {
            restoreFromBackup(shardUuid, file);
        }

        try {
            return new FileOrcDataSource(file, orcMaxMergeDistance);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to open shard file: " + file, e);
        }
    }

    private void restoreFromBackup(UUID shardUuid, File file)
    {
        File backupFile = getBackupFile(shardUuid);

        // create a temporary file in the staging directory
        File stagingFile = temporarySuffix(getStagingFile(shardUuid));
        createParents(stagingFile);

        // copy to temporary file
        log.info("Copying shard %s from backup...", shardUuid);
        long start = System.nanoTime();

        try {
            Files.copy(backupFile.toPath(), stagingFile.toPath());
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to copy backup shard file: " + backupFile, e);
        }

        Duration duration = nanosSince(start);
        DataSize size = new DataSize(stagingFile.length(), BYTE);
        DataSize rate = dataRate(size, duration);
        log.info("Copied shard %s from backup in %s (%s at %s/s)", shardUuid, duration, size, rate);

        // move to final location
        createParents(file);
        try {
            Files.move(stagingFile.toPath(), file.toPath(), ATOMIC_MOVE);
        }
        catch (FileAlreadyExistsException e) {
            // someone else already created it (should not happen, but safe to ignore)
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to move shard file", e);
        }
    }

    private static OrcPredicate getPredicate(TupleDomain<RaptorColumnHandle> effectivePredicate, Map<Long, Integer> indexMap)
    {
        ImmutableList.Builder<ColumnReference<RaptorColumnHandle>> columns = ImmutableList.builder();
        for (RaptorColumnHandle column : effectivePredicate.getDomains().keySet()) {
            Integer index = indexMap.get(column.getColumnId());
            if (index != null) {
                columns.add(new ColumnReference<>(column, index, column.getColumnType()));
            }
        }
        return new TupleDomainOrcPredicate<>(effectivePredicate, columns.build());
    }

    private static Map<Long, Integer> columnIdIndex(List<String> columnNames)
    {
        ImmutableMap.Builder<Long, Integer> map = ImmutableMap.builder();
        for (int i = 0; i < columnNames.size(); i++) {
            map.put(Long.valueOf(columnNames.get(i)), i);
        }
        return map.build();
    }

    private static File temporarySuffix(File file)
    {
        return new File(file.getPath() + ".tmp-" + UUID.randomUUID());
    }

    private static void createParents(File file)
    {
        File dir = file.getParentFile();
        try {
            createDirectories(dir.toPath());
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed creating directories: " + dir, e);
        }
    }

    private static List<StorageType> toStorageTypes(List<Type> columnTypes)
    {
        return FluentIterable.from(columnTypes)
                .transform(new Function<Type, StorageType>()
                {
                    @Override
                    public StorageType apply(Type type)
                    {
                        return toStorageType(type);
                    }
                })
                .toList();
    }

    private static StorageType toStorageType(Type type)
    {
        Class<?> javaType = type.getJavaType();
        if (javaType == boolean.class) {
            return StorageType.BOOLEAN;
        }
        if (javaType == long.class) {
            return StorageType.LONG;
        }
        if (javaType == double.class) {
            return StorageType.DOUBLE;
        }
        if (javaType == Slice.class) {
            if (type.equals(VarcharType.VARCHAR)) {
                return StorageType.STRING;
            }
            if (type.equals(VarbinaryType.VARBINARY)) {
                return StorageType.BYTES;
            }
        }
        throw new PrestoException(NOT_SUPPORTED, "No storage type for type: " + type);
    }

    /**
     * Generate a file system path for a shard UUID.
     * <p/>
     * This creates a three level deep directory structure where the first
     * two levels each contain three hex digits (lowercase) of the UUID
     * and the final level contains the full UUID.
     * Example:
     * <p/>
     * <pre>
     * UUID: 701e1a79-74f7-4f56-b438-b41e8e7d019d
     * Path: /base/701/e1a/701e1a79-74f7-4f56-b438-b41e8e7d019d.orc
     * </pre>
     * <p/>
     * This ensures that files are spread out evenly through the tree
     * while a path can still be easily navigated by a human being.
     */
    private static File getFileSystemPath(File base, UUID shardUuid)
    {
        String uuid = shardUuid.toString().toLowerCase(ENGLISH);
        return base.toPath()
                .resolve(uuid.substring(0, 3))
                .resolve(uuid.substring(3, 6))
                .resolve(uuid + ".orc")
                .toFile();
    }

    private static void deleteDirectory(File dir)
            throws IOException
    {
        if (!dir.exists()) {
            return;
        }
        File[] files = dir.listFiles();
        if (files == null) {
            throw new IOException("Failed to list directory: " + dir);
        }
        for (File file : files) {
            Files.delete(file.toPath());
        }
        Files.delete(dir.toPath());
    }

    private static DataSize dataRate(DataSize size, Duration duration)
    {
        double rate = size.toBytes() / duration.getValue(SECONDS);
        if (Double.isNaN(rate) || Double.isInfinite(rate)) {
            rate = 0;
        }
        return new DataSize(rate, BYTE).convertToMostSuccinctDataSize();
    }
}
