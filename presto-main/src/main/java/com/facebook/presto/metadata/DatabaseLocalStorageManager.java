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
package com.facebook.presto.metadata;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockUtils;
import com.facebook.presto.execution.TaskId;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.serde.BlocksFileReader;
import com.facebook.presto.serde.BlocksFileStats;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.util.KeyBoundedExecutor;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.inject.Inject;
import io.airlift.concurrent.ThreadPoolExecutorMBean;
import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;
import org.weakref.jmx.Managed;
import org.weakref.jmx.Nested;

import javax.annotation.PreDestroy;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;

import static com.facebook.presto.util.Threads.threadsNamed;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class DatabaseLocalStorageManager
        implements LocalStorageManager
{
    private static final boolean ENABLE_OPTIMIZATION = Boolean.valueOf("false");

    private static final BlocksFileEncoding DEFAULT_ENCODING = BlocksFileEncoding.SNAPPY;

    private static final int RUN_LENGTH_AVERAGE_CUTOFF = 3;
    private static final int DICTIONARY_CARDINALITY_CUTOFF = 1000;

    private static final Logger log = Logger.get(DatabaseLocalStorageManager.class);

    private final ExecutorService executor;
    private final ThreadPoolExecutorMBean executorMBean;
    private final KeyBoundedExecutor<UUID> shardBoundedExecutor;

    private final IDBI dbi;
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
    public DatabaseLocalStorageManager(@ForLocalStorageManager IDBI dbi, DatabaseLocalStorageManagerConfig config)
            throws IOException
    {
        checkNotNull(config, "config is null");
        File baseDataDir = checkNotNull(config.getDataDirectory(), "dataDirectory is null");
        this.baseStorageDir = createDirectory(new File(baseDataDir, "storage"));
        this.baseStagingDir = createDirectory(new File(baseDataDir, "staging"));
        this.dbi = checkNotNull(dbi, "dbi is null");
        this.dao = dbi.onDemand(StorageManagerDao.class);

        this.executor = newFixedThreadPool(config.getTasksPerNode(), threadsNamed("local-storage-manager-%s"));
        this.executorMBean = new ThreadPoolExecutorMBean((ThreadPoolExecutor) executor);
        this.shardBoundedExecutor = new KeyBoundedExecutor<>(executor);

        dao.createTableColumns();
    }

    @PreDestroy
    public void stop()
    {
        executor.shutdown();
    }

    @Managed
    @Nested
    public ThreadPoolExecutorMBean getExecutor()
    {
        return executorMBean;
    }

    @Override
    public ColumnFileHandle createStagingFileHandles(UUID shardUuid, List<? extends ColumnHandle> columnHandles)
            throws IOException
    {
        File shardPath = getShardPath(baseStagingDir, shardUuid);

        ColumnFileHandle.Builder builder = ColumnFileHandle.builder(shardUuid);

        for (ColumnHandle columnHandle : columnHandles) {
            File file = getColumnFile(shardPath, columnHandle, DEFAULT_ENCODING);
            Files.createParentDirs(file);
            builder.addColumn(columnHandle, file, DEFAULT_ENCODING);
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

        ImmutableList.Builder<BlockIterable> sourcesBuilder = ImmutableList.builder();
        ColumnFileHandle.Builder builder = ColumnFileHandle.builder(shardUuid);

        for (Map.Entry<ColumnHandle, File> entry : columnFileHandle.getFiles().entrySet()) {
            File file = entry.getValue();
            ColumnHandle columnHandle = entry.getKey();

            if (file.length() > 0) {
                Slice slice = mappedFileCache.getUnchecked(file.getAbsoluteFile());
                checkState(file.length() == slice.length(), "File %s, length %s was mapped to Slice length %s", file.getAbsolutePath(), file.length(), slice.length());
                // Compute optimal encoding from stats
                BlocksFileReader blocks = BlocksFileReader.readBlocks(slice);
                BlocksFileStats stats = blocks.getStats();
                boolean rleEncode = stats.getAvgRunLength() > RUN_LENGTH_AVERAGE_CUTOFF;
                boolean dicEncode = stats.getUniqueCount() < DICTIONARY_CARDINALITY_CUTOFF;

                BlocksFileEncoding encoding = DEFAULT_ENCODING;

                if (ENABLE_OPTIMIZATION) {
                    if (dicEncode && rleEncode) {
                        encoding = BlocksFileEncoding.DIC_RLE;
                    }
                    else if (dicEncode) {
                        encoding = BlocksFileEncoding.DIC_RAW;
                    }
                    else if (rleEncode) {
                        encoding = BlocksFileEncoding.RLE;
                    }
                }

                File outputFile = getColumnFile(shardPath, columnHandle, encoding);
                Files.createParentDirs(outputFile);

                if (encoding == DEFAULT_ENCODING) {
                    // Optimization: source is already raw, so just move.
                    Files.move(file, outputFile);
                    // still register the file with the builder so that it can
                    // be committed correctly.
                    builder.addColumn(columnHandle, outputFile);
                }
                else {
                    // source builder and output builder move in parallel if the
                    // column gets written
                    sourcesBuilder.add(blocks);
                    builder.addColumn(columnHandle, outputFile, encoding);
                }
            }
            else {
                // fake file
                File outputFile = getColumnFile(shardPath, columnHandle, DEFAULT_ENCODING);
                builder.addColumn(columnHandle, outputFile);
            }
        }

        List<BlockIterable> sources = sourcesBuilder.build();
        ColumnFileHandle targetFileHandle = builder.build();

        if (!sources.isEmpty()) {
            // Throw out any stats generated by the optimization setp
            Session session = new Session("user", "source", "catalog", "schema", "address", "agent");
            OperatorContext operatorContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session)
                    .addPipelineContext(true, true)
                    .addDriverContext()
                    .addOperatorContext(0, "OptimizeEncodings");

            AlignmentOperator source = new AlignmentOperator(operatorContext, sources);
            importData(source, targetFileHandle);
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

    private static void importData(AlignmentOperator source, ColumnFileHandle fileHandle)
    {
        while (!source.isFinished()) {
            Page page = source.getOutput();
            if (page != null) {
                fileHandle.append(page);
            }
            checkState(source.isBlocked().isDone(), "Alignment operator is blocked");
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
        String uuid = shardUuid.toString().toLowerCase();
        return baseDir.toPath()
                .resolve(uuid.substring(0, 2))
                .resolve(uuid.substring(2, 4))
                .resolve(uuid.substring(4, 6))
                .resolve(uuid)
                .toFile();
    }

    private static File getColumnFile(File shardPath, ColumnHandle columnHandle, BlocksFileEncoding encoding)
    {
        checkState(columnHandle instanceof NativeColumnHandle, "Can only import in a native column");
        long columnId = ((NativeColumnHandle) columnHandle).getColumnId();
        return new File(shardPath, format("%s.%s.column", columnId, encoding.getName()));
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

                for (Map.Entry<ColumnHandle, File> entry : columnFileHandle.getFiles().entrySet()) {
                    ColumnHandle columnHandle = entry.getKey();
                    File file = entry.getValue();

                    checkState(columnHandle instanceof NativeColumnHandle, "Can only import in a native column");
                    long columnId = ((NativeColumnHandle) columnHandle).getColumnId();
                    String filename = file.getName();
                    dao.insertColumn(columnFileHandle.getShardUuid(), columnId, filename);
                }
            }
        });
    }

    @Override
    public BlockIterable getBlocks(UUID shardUuid, ColumnHandle columnHandle)
    {
        checkNotNull(columnHandle);
        checkState(columnHandle instanceof NativeColumnHandle, "Can only load blocks from a native column");
        long columnId = ((NativeColumnHandle) columnHandle).getColumnId();

        checkState(shardExists(shardUuid), "shard %s does not exist in local database", shardUuid);
        String filename = dao.getColumnFilename(shardUuid, columnId);
        File file = new File(getShardPath(baseStorageDir, shardUuid), filename);

        // TODO: remove this hack when empty blocks are allowed
        if (!file.exists()) {
            return BlockUtils.emptyBlockIterable();
        }

        return convertFilesToBlocks(ImmutableList.of(file));
    }

    private BlockIterable convertFilesToBlocks(Iterable<File> files)
    {
        checkArgument(files.iterator().hasNext(), "no files in stream");

        Iterable<Block> blocks = Iterables.concat(Iterables.transform(files, new Function<File, Iterable<? extends Block>>()
        {
            @Override
            public Iterable<? extends Block> apply(File file)
            {
                Slice slice = mappedFileCache.getUnchecked(file.getAbsoluteFile());
                return BlocksFileReader.readBlocks(slice);
            }
        }));

        return BlockUtils.toBlocks(blocks);
    }

    @Override
    public boolean shardExists(UUID shardUuid)
    {
        return dao.shardExists(shardUuid);
    }

    @Override
    public void dropShard(UUID shardUuid)
    {
        shardBoundedExecutor.execute(shardUuid, new DropJob(shardUuid));
    }

    @Override
    public boolean isShardActive(UUID shardUuid)
    {
        return shardBoundedExecutor.isActive(shardUuid);
    }

    private static File createDirectory(File dir)
            throws IOException
    {
        createDirectories(dir.toPath());
        return dir;
    }

    private class DropJob
            implements Runnable
    {
        private final UUID shardUuid;

        private DropJob(UUID shardUuid)
        {
            this.shardUuid = checkNotNull(shardUuid, "shardUuid is null");
        }

        @Override
        public void run()
        {
            // TODO: dropping needs to be globally coordinated with read queries
            List<String> shardFiles = dao.getShardFiles(shardUuid);
            for (String shardFile : shardFiles) {
                File file = new File(getShardPath(baseStorageDir, shardUuid), shardFile);
                if (!file.delete()) {
                    log.warn("failed to delete file: %s", file.getAbsolutePath());
                }
            }
            dao.dropShard(shardUuid);
        }
    }
}
