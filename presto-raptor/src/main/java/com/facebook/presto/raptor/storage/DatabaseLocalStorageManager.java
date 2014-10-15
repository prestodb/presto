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

import com.facebook.presto.raptor.RaptorColumnHandle;
import com.facebook.presto.raptor.block.BlockPageSource;
import com.facebook.presto.raptor.block.BlocksFileReader;
import com.facebook.presto.raptor.block.NoColumnsPageSource;
import com.facebook.presto.spi.ConnectorColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.facebook.presto.raptor.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.util.Locale.ENGLISH;

public class DatabaseLocalStorageManager
        implements LocalStorageManager
{
    private final IDBI dbi;
    private final BlockEncodingSerde blockEncodingSerde;
    private final File baseStorageDir;
    private final File baseStagingDir;
    private final StorageManagerDao dao;

    private final LoadingCache<File, Slice> mappedFileCache = CacheBuilder.newBuilder().build(new CacheLoader<File, Slice>()
    {
        @Override
        public Slice load(File file)
                throws Exception
        {
            checkArgument(file.isAbsolute(), "file is not absolute: %s", file);
            checkArgument(file.canRead(), "file is not readable: %s", file);
            if (file.length() == 0) {
                return Slices.EMPTY_SLICE;
            }
            return Slices.mapFileReadOnly(file);
        }
    });

    @Inject
    public DatabaseLocalStorageManager(@ForLocalStorageManager IDBI dbi, BlockEncodingSerde blockEncodingSerde, DatabaseLocalStorageManagerConfig config)
            throws IOException
    {
        this.blockEncodingSerde = checkNotNull(blockEncodingSerde, "blockEncodingManager is null");

        checkNotNull(config, "config is null");
        File baseDataDir = checkNotNull(config.getDataDirectory(), "dataDirectory is null");
        this.baseStorageDir = createDirectory(new File(baseDataDir, "storage"));
        this.baseStagingDir = createDirectory(new File(baseDataDir, "staging"));
        this.dbi = checkNotNull(dbi, "dbi is null");
        this.dao = dbi.onDemand(StorageManagerDao.class);

        dao.createTableColumns();
    }

    @Override
    public ColumnFileHandle createStagingFileHandles(UUID shardUuid, List<RaptorColumnHandle> columnHandles)
            throws IOException
    {
        File shardPath = getShardPath(baseStagingDir, shardUuid);

        ColumnFileHandle.Builder builder = ColumnFileHandle.builder(shardUuid, blockEncodingSerde);

        for (RaptorColumnHandle columnHandle : columnHandles) {
            File file = getColumnFile(shardPath, columnHandle);
            Files.createParentDirs(file);
            builder.addColumn(columnHandle, file);
        }

        return builder.build();
    }

    @Override
    public void commit(ColumnFileHandle columnFileHandle)
            throws IOException
    {
        checkNotNull(columnFileHandle, "columnFileHandle is null");

        columnFileHandle.commit();

        // Process staged files to optimize encodings if necessary
        ColumnFileHandle finalColumnFileHandle = optimizeEncodings(columnFileHandle);

        // Commit all the columns at the same time once everything has been successfully imported
        commitShardColumns(finalColumnFileHandle);

        // Delete empty staging directory
        deleteStagingDirectory(columnFileHandle);
    }

    private ColumnFileHandle optimizeEncodings(ColumnFileHandle columnFileHandle)
            throws IOException
    {
        UUID shardUuid = columnFileHandle.getShardUuid();
        File shardPath = getShardPath(baseStorageDir, shardUuid);

        ImmutableList.Builder<Iterable<Block>> sourcesBuilder = ImmutableList.builder();
        ColumnFileHandle.Builder builder = ColumnFileHandle.builder(shardUuid, blockEncodingSerde);

        for (Map.Entry<ConnectorColumnHandle, File> entry : columnFileHandle.getFiles().entrySet()) {
            File file = entry.getValue();
            RaptorColumnHandle columnHandle = checkType(entry.getKey(), RaptorColumnHandle.class, "columnHandle");

            if (file.length() > 0) {
                Slice slice = mappedFileCache.getUnchecked(file.getAbsoluteFile());
                checkState(file.length() == slice.length(), "File %s, length %s was mapped to Slice length %s", file.getAbsolutePath(), file.length(), slice.length());

                File outputFile = getColumnFile(shardPath, columnHandle);
                Files.createParentDirs(outputFile);

                Files.move(file, outputFile);
                builder.addExistingColumn(columnHandle, outputFile);
            }
            else {
                // fake file
                File outputFile = getColumnFile(shardPath, columnHandle);
                builder.addExistingColumn(columnHandle, outputFile);
            }
        }

        List<Iterable<Block>> sources = sourcesBuilder.build();
        ColumnFileHandle targetFileHandle = builder.build();

        if (!sources.isEmpty()) {
            // Throw out any stats generated by the optimization step
            importData(new BlockPageSource(sources), targetFileHandle);
        }

        targetFileHandle.commit();

        return targetFileHandle;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteStagingDirectory(ColumnFileHandle columnFileHandle)
    {
        File path = getShardPath(baseStagingDir, columnFileHandle.getShardUuid());

        while (path.delete() && !path.getParentFile().equals(baseStagingDir)) {
            path = path.getParentFile();
        }
    }

    private static void importData(ConnectorPageSource source, ColumnFileHandle fileHandle)
    {
        while (!source.isFinished()) {
            Page page = source.getNextPage();
            if (page != null) {
                fileHandle.append(page);
            }
        }
    }

    /**
     * Generate a file system path for a shard UUID.
     * <p/>
     * This creates a four level deep directory structure where the first
     * three levels each contain two hex digits (lowercase) of the UUID
     * and the final level contains the full UUID.
     * Example:
     * <p/>
     * <pre>
     * UUID: db298a0c-e968-4d5a-8e58-b1021c7eab2c
     * Path: db/29/8a/db298a0c-e968-4d5a-8e58-b1021c7eab2c
     * </pre>
     * <p/>
     * This ensures that files are spread out evenly through the tree
     * while a path can still be easily navigated by a human being.
     */
    @VisibleForTesting
    static File getShardPath(File baseDir, UUID shardUuid)
    {
        String uuid = shardUuid.toString().toLowerCase(ENGLISH);
        return baseDir.toPath()
                .resolve(uuid.substring(0, 2))
                .resolve(uuid.substring(2, 4))
                .resolve(uuid.substring(4, 6))
                .resolve(uuid)
                .toFile();
    }

    private static File getColumnFile(File shardPath, ConnectorColumnHandle columnHandle)
    {
        long columnId = checkType(columnHandle, RaptorColumnHandle.class, "columnHandle").getColumnId();
        return new File(shardPath, format("%s.%s.column", columnId, "raw"));
    }

    private void commitShardColumns(final ColumnFileHandle columnFileHandle)
    {
        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
                    throws Exception
            {
                StorageManagerDao dao = handle.attach(StorageManagerDao.class);

                for (Map.Entry<ConnectorColumnHandle, File> entry : columnFileHandle.getFiles().entrySet()) {
                    ConnectorColumnHandle columnHandle = entry.getKey();
                    File file = entry.getValue();

                    long columnId = checkType(columnHandle, RaptorColumnHandle.class, "columnHandle").getColumnId();
                    String filename = file.getName();
                    dao.insertColumn(columnFileHandle.getShardUuid(), columnId, filename);
                }
            }
        });
    }

    @Override
    public ConnectorPageSource getPageSource(UUID shardUuid, List<Long> columnIds, long countColumnId)
    {
        if (columnIds.isEmpty()) {
            return createNoColumnsOperator(shardUuid, countColumnId);
        }
        return createBlockPageSource(shardUuid, columnIds);
    }

    private ConnectorPageSource createNoColumnsOperator(UUID shardUuid, long countColumnId)
    {
        Iterable<Block> blocks = getBlocks(shardUuid, countColumnId);
        return new NoColumnsPageSource(Iterables.transform(blocks, new Function<Block, Integer>()
        {
            @Override
            public Integer apply(Block input)
            {
                return input.getPositionCount();
            }
        }));
    }

    public ConnectorPageSource createBlockPageSource(UUID shardUuid, List<Long> columnIds)
    {
        ImmutableList.Builder<Iterable<Block>> channels = ImmutableList.builder();
        for (long columnId : columnIds) {
            channels.add(getBlocks(shardUuid, columnId));
        }
        return new BlockPageSource(channels.build());
    }

    private Iterable<Block> getBlocks(UUID shardUuid, long columnId)
    {
        checkState(shardExists(shardUuid), "shard %s does not exist in local database", shardUuid);
        String filename = dao.getColumnFilename(shardUuid, columnId);
        File file = new File(getShardPath(baseStorageDir, shardUuid), filename);

        if (!file.exists()) {
            return ImmutableList.of();
        }

        return convertFilesToBlocks(ImmutableList.of(file));
    }

    @SuppressWarnings("UnnecessaryLocalVariable")
    private Iterable<Block> convertFilesToBlocks(Iterable<File> files)
    {
        checkArgument(files.iterator().hasNext(), "no files in stream");

        Iterable<Block> blocks = Iterables.concat(Iterables.transform(files, new Function<File, Iterable<? extends Block>>()
        {
            @Override
            public Iterable<? extends Block> apply(File file)
            {
                Slice slice = mappedFileCache.getUnchecked(file.getAbsoluteFile());
                return BlocksFileReader.readBlocks(blockEncodingSerde, slice);
            }
        }));

        return blocks;
    }

    @Override
    public boolean shardExists(UUID shardUuid)
    {
        return dao.shardExists(shardUuid);
    }

    private static File createDirectory(File dir)
            throws IOException
    {
        createDirectories(dir.toPath());
        return dir;
    }
}
