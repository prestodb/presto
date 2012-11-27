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
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.io.Files;
import com.google.inject.Inject;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.IDBI;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.VoidTransactionCallback;

import java.io.File;
import java.io.IOException;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static java.lang.String.format;

public class DatabaseStorageManager
        implements StorageManager
{
    private static final boolean enableOptimization = Boolean.valueOf("false");

    private static final File DEFAULT_BASE_STORAGE_DIR = new File("var/presto-data/storage");
    private static final File DEFAULT_BASE_STAGING_DIR = new File("var/presto-data/staging");
    private static final int RUN_LENGTH_AVERAGE_CUTOFF = 3;
    private static final int DICTIONARY_CARDINALITY_CUTOFF = 1000;

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
            return Slices.mapFileReadOnly(file);
        }
    });

    public DatabaseStorageManager(IDBI dbi, File baseStorageDir, File baseStagingDir)
    {
        this.dbi = checkNotNull(dbi, "iDbi is null");
        this.baseStorageDir = checkNotNull(baseStorageDir, "baseStorageDir is null");
        this.baseStagingDir = checkNotNull(baseStagingDir, "baseStagingDir is null");
        this.dao = dbi.onDemand(StorageManagerDao.class);

        dao.createTableColumns();
    }

    @Inject
    public DatabaseStorageManager(@ForStorageManager IDBI dbi)
    {
        this(dbi, DEFAULT_BASE_STORAGE_DIR, DEFAULT_BASE_STAGING_DIR);
    }

    @Override
    public void importShard(long shardId, List<Long> columnIds, Operator source)
            throws IOException
    {
        checkArgument(source.getChannelCount() == columnIds.size(), "channel count does not match columnId list");

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
        List<File> outputFiles = getOuputFiles(shardPath, columnIds);
        List<BlocksFileWriter> writers = getFileWriters(outputFiles);

        ImportingOperator.importData(source, writers);

        return outputFiles;
    }

    private static List<File> getOuputFiles(File shardPath, List<Long> columnIds)
            throws IOException
    {
        ImmutableList.Builder<File> files = ImmutableList.builder();
        for (long columnId : columnIds) {
            File file = getColumnFile(shardPath, columnId, BlocksFileEncoding.RAW);
            Files.createParentDirs(file);
            files.add(file);
        }
        return files.build();
    }

    private static List<BlocksFileWriter> getFileWriters(List<File> files)
    {
        ImmutableList.Builder<BlocksFileWriter> writers = ImmutableList.builder();
        for (File file : files) {
            writers.add(new BlocksFileWriter(BlocksFileEncoding.RAW, Files.newOutputStreamSupplier(file)));
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

            BlocksFileEncoding encoding = BlocksFileEncoding.RAW;
            if (dicEncode && rleEncode) {
                encoding = BlocksFileEncoding.DIC_RLE;
            }
            else if (dicEncode) {
                encoding = BlocksFileEncoding.DIC_RAW;
            }
            else if (rleEncode) {
                encoding = BlocksFileEncoding.RLE;
            }
            if (!enableOptimization) {
                encoding = BlocksFileEncoding.RAW;
            }

            File outputFile = getColumnFile(shardPath, columnId, encoding);
            Files.createParentDirs(outputFile);
            optimizedFilesBuilder.add(outputFile);

            if (encoding == BlocksFileEncoding.RAW) {
                // Should already be raw, so just move
                Files.move(stagedFile, outputFile);
            }
            else {
                sourcesBuilder.add(blocks);
                writersBuilder.add(new BlocksFileWriter(encoding, Files.newOutputStreamSupplier(outputFile)));
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

    private static File getShardPath(File baseDir, long shardId)
    {
        return new File(baseDir, String.valueOf(shardId));
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
        String filename = dao.getColumnFilename(shardId, columnId);
        File file = new File(getShardPath(baseStorageDir, shardId), filename);

        // TODO: remove this hack when empty blocks are allowed
        if (!file.exists()) {
            return BlockUtils.emptyBlockIterable();
        }

        return convertFilesToBlocks(ImmutableList.of(file));
    }

    private BlockIterable convertFilesToBlocks(List<File> files)
    {
        Preconditions.checkArgument(!files.isEmpty(), "no files in stream");

        List<Block> blocks = ImmutableList.copyOf(Iterables.concat(Iterables.transform(files, new Function<File, Iterable<? extends Block>>()
        {
            @Override
            public Iterable<? extends Block> apply(File file)
            {
                Slice slice = mappedFileCache.getUnchecked(file.getAbsoluteFile());
                return BlocksFileReader.readBlocks(slice);
            }
        })));

        return BlockUtils.toBlocks(blocks);
    }

    @Override
    public boolean shardExists(long shardId)
    {
        return dao.shardExists(shardId);
    }
}
