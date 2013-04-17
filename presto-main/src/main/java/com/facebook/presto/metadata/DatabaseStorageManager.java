package com.facebook.presto.metadata;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockUtils;
import com.facebook.presto.ingest.ImportingOperator;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.serde.BlocksFileReader;
import com.facebook.presto.serde.BlocksFileStats;
import com.facebook.presto.serde.BlocksFileWriter;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.OutputSupplier;
import com.google.inject.Inject;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.units.DataSize.Unit.KILOBYTE;
import static java.lang.String.format;
import static java.nio.file.Files.createDirectories;

public class DatabaseStorageManager
        implements StorageManager
{
    private static final boolean ENABLE_OPTIMIZATION = Boolean.valueOf("false");

    private static final BlocksFileEncoding DEFAULT_ENCODING = BlocksFileEncoding.SNAPPY;

    private static final int RUN_LENGTH_AVERAGE_CUTOFF = 3;
    private static final int DICTIONARY_CARDINALITY_CUTOFF = 1000;
    private static final int OUTPUT_BUFFER_SIZE = (int) new DataSize(64, KILOBYTE).toBytes();

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
            checkArgument(file.isAbsolute(), "file is not absolute");
            if (file.exists() && file.length() > 0) {
                return Slices.mapFileReadOnly(file);
            }
            else {
                return Slices.EMPTY_SLICE;
            }
        }
    });

    @Inject
    public DatabaseStorageManager(@ForStorageManager IDBI dbi, StorageManagerConfig config)
            throws IOException
    {
        checkNotNull(config, "config is null");
        File baseDataDir = checkNotNull(config.getDataDirectory(), "dataDirectory is null");
        this.baseStorageDir = createDirectory(new File(baseDataDir, "storage"));
        this.baseStagingDir = createDirectory(new File(baseDataDir, "staging"));
        this.dbi = checkNotNull(dbi, "dbi is null");
        this.dao = dbi.onDemand(StorageManagerDao.class);

        dao.createTableColumns();
    }

    @Override
    public void importShard(long shardId, List<Long> columnIds, Operator source)
            throws IOException
    {
        checkArgument(source.getChannelCount() == columnIds.size(), "channel count does not match columnId list");
        checkState(!shardExists(shardId), "shard %s has already been imported", shardId);

        // Locally stage the imported data
        List<File> files = stagingImport(shardId, columnIds, source);

        // Process staged files to optimize encodings if necessary
        List<File> finalOutputFiles = optimizeEncodings(shardId, columnIds, files);

        // Commit all the columns at the same time once everything has been successfully imported
        commitShardColumns(shardId, columnIds, finalOutputFiles);

        // Delete empty staging directory
        deleteStagingDirectory(shardId);
    }

    private List<File> stagingImport(long shardId, List<Long> columnIds, Operator source)
            throws IOException
    {
        File shardPath = getShardPath(baseStagingDir, shardId);
        List<File> outputFiles = getOutputFiles(shardPath, columnIds);
        List<BlocksFileWriter> writers = getFileWriters(outputFiles);

        ImportingOperator.importData(source, writers);

        return outputFiles;
    }

    private static List<File> getOutputFiles(File shardPath, List<Long> columnIds)
            throws IOException
    {
        ImmutableList.Builder<File> files = ImmutableList.builder();
        for (long columnId : columnIds) {
            File file = getColumnFile(shardPath, columnId, DEFAULT_ENCODING);
            Files.createParentDirs(file);
            files.add(file);
        }
        return files.build();
    }

    private static List<BlocksFileWriter> getFileWriters(List<File> files)
    {
        ImmutableList.Builder<BlocksFileWriter> writers = ImmutableList.builder();
        for (File file : files) {
            writers.add(new BlocksFileWriter(DEFAULT_ENCODING, createOutputSupplier(file)));
        }
        return writers.build();
    }

    private List<File> optimizeEncodings(long shardId, List<Long> columnIds, List<File> stagedFiles)
            throws IOException
    {
        checkArgument(columnIds.size() == stagedFiles.size(), "columnId list does not match file list");

        ImmutableList.Builder<BlockIterable> sourcesBuilder = ImmutableList.builder();
        ImmutableList.Builder<BlocksFileWriter> writersBuilder = ImmutableList.builder();
        ImmutableList.Builder<File> optimizedFilesBuilder = ImmutableList.builder();

        File shardPath = getShardPath(baseStorageDir, shardId);

        // TODO: remove this hack when empty blocks are allowed
        if (!stagedFiles.get(0).exists()) {
            ImmutableList.Builder<File> outputFiles = ImmutableList.builder();
            for (File file : stagedFiles) {
                outputFiles.add(new File(shardPath, file.getName()));
            }
            return outputFiles.build();
        }

        for (int i = 0; i < stagedFiles.size(); i++) {
            long columnId = columnIds.get(i);
            File stagedFile = stagedFiles.get(i);
            Slice slice = mappedFileCache.getUnchecked(stagedFile.getAbsoluteFile());

            // Compute optimal encoding from stats
            BlocksFileReader blocks = BlocksFileReader.readBlocks(slice);
            BlocksFileStats stats = blocks.getStats();
            boolean rleEncode = stats.getAvgRunLength() > RUN_LENGTH_AVERAGE_CUTOFF;
            boolean dicEncode = stats.getUniqueCount() < DICTIONARY_CARDINALITY_CUTOFF;

            BlocksFileEncoding encoding = DEFAULT_ENCODING;
            if (dicEncode && rleEncode) {
                encoding = BlocksFileEncoding.DIC_RLE;
            }
            else if (dicEncode) {
                encoding = BlocksFileEncoding.DIC_RAW;
            }
            else if (rleEncode) {
                encoding = BlocksFileEncoding.RLE;
            }
            if (!ENABLE_OPTIMIZATION) {
                encoding = DEFAULT_ENCODING;
            }

            File outputFile = getColumnFile(shardPath, columnId, encoding);
            Files.createParentDirs(outputFile);
            optimizedFilesBuilder.add(outputFile);

            if (encoding == DEFAULT_ENCODING) {
                // Should already be raw, so just move
                Files.move(stagedFile, outputFile);
            }
            else {
                sourcesBuilder.add(blocks);
                writersBuilder.add(new BlocksFileWriter(encoding, createOutputSupplier(outputFile)));
            }
        }
        List<BlockIterable> sources = sourcesBuilder.build();
        List<BlocksFileWriter> writers = writersBuilder.build();

        if (!sources.isEmpty()) {
            AlignmentOperator source = new AlignmentOperator(sources);
            ImportingOperator.importData(source, writers);
        }
        return optimizedFilesBuilder.build();
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteStagingDirectory(long shardId)
    {
        getShardPath(baseStagingDir, shardId).delete();
    }

    private static OutputSupplier<OutputStream> createOutputSupplier(final File file)
    {
        return new OutputSupplier<OutputStream>()
        {
            @Override
            public OutputStream getOutput()
                    throws IOException
            {
                return new BufferedOutputStream(new FileOutputStream(file), OUTPUT_BUFFER_SIZE);
            }
        };
    }

    /**
     * Generate a file system path for a shard id. This creates a four level deep, two digit directory
     * where the least significant digits are the first level, the next significant digits are the second
     * and so on. Numbers that have more than eight digits are lumped together in the last level.
     *
     * <pre>
     *   1 --> 01/00/00/00
     *   1000 -> 00/10/00/00
     *   123456 -> 56/34/12/00
     *   4815162342 -> 42/23/16/4815
     * </pre>
     *
     * This ensures that files are spread out evenly through the tree while a path can still be easily navigated
     * by a human being.
     *
     * @param baseDir
     * @param shardId
     * @return
     */
    @VisibleForTesting
    static File getShardPath(File baseDir, long shardId)
    {
        Preconditions.checkArgument(shardId >= 0, "shardId must be >= 0");

        String value = format("%08d", shardId);
        int split = value.length() - 6;
        List<String> pathElements = ImmutableList.copyOf(Splitter.fixedLength(2).limit(3).split(value.substring(split)));
        String path = Joiner.on('/').join(Lists.reverse(pathElements)) + "/" + value.substring(0, split);
        return new File(baseDir, path);
    }

    private static File getColumnFile(File shardPath, long columnId, BlocksFileEncoding encoding)
    {
        return new File(shardPath, format("%s.%s.column", columnId, encoding.getName()));
    }

    private void commitShardColumns(final long shardId, final List<Long> columnIds, final List<File> files)
    {
        checkArgument(columnIds.size() == files.size(), "columnId list does not match file list");
        dbi.inTransaction(new VoidTransactionCallback()
        {
            @Override
            protected void execute(Handle handle, TransactionStatus status)
                    throws Exception
            {
                StorageManagerDao dao = handle.attach(StorageManagerDao.class);
                for (int i = 0; i < columnIds.size(); i++) {
                    long columnId = columnIds.get(i);
                    String filename = files.get(i).getName();
                    dao.insertColumn(shardId, columnId, filename);
                }
            }
        });
    }

    @Override
    public BlockIterable getBlocks(long shardId, long columnId)
    {
        checkState(shardExists(shardId), "shard %s has not yet been imported", shardId);
        String filename = dao.getColumnFilename(shardId, columnId);
        File file = new File(getShardPath(baseStorageDir, shardId), filename);

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
    public boolean shardExists(long shardId)
    {
        return dao.shardExists(shardId);
    }

    @Override
    public void dropShard(long shardId)
            throws IOException
    {
        // TODO: dropping needs to be globally coordinated with read queries
        List<String> shardFiles = dao.getShardFiles(shardId);
        for (String shardFile : shardFiles) {
            File file = new File(getShardPath(baseStorageDir, shardId), shardFile);
            java.nio.file.Files.deleteIfExists(file.toPath());
        }
        dao.dropShard(shardId);
    }

    private static File createDirectory(File dir)
            throws IOException
    {
        createDirectories(dir.toPath());
        return dir;
    }
}
