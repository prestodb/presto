package com.facebook.presto.metadata;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.block.BlockUtils;
import com.facebook.presto.ingest.ImportingOperator;
import com.facebook.presto.operator.AlignmentOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.serde.BlocksFileWriter;
import com.facebook.presto.serde.BlocksFileReader;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.serde.BlocksFileStats;
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
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionIsolationLevel;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.tweak.HandleCallback;
import org.skife.jdbi.v2.tweak.ResultSetMapper;

import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class DatabaseStorageManager
        implements StorageManager
{
    private static final File DEFAULT_BASE_STORAGE_DIR = new File("var/presto-data/storage");
    private static final File DEFAULT_BASE_STAGING_DIR = new File("var/presto-data/staging");
    private static final int RUN_LENGTH_AVERAGE_CUTOFF = 3;
    private static final int DICTIONARY_CARDINALITY_CUTOFF = 1000;

    private final IDBI dbi;
    private final File baseStorageDir;
    private final File baseStagingDir;
    private final LoadingCache<String, Slice> mappedFileCache = CacheBuilder.newBuilder().build(new CacheLoader<String, Slice>(){
        @Override
        public Slice load(String key)
                throws Exception
        {
            File file = new File(key);
            Slice slice = Slices.mapFileReadOnly(file);
            return slice;
        }
    });

    public DatabaseStorageManager(IDBI dbi, File baseStorageDir, File baseStagingDir)
    {
        this.dbi = Preconditions.checkNotNull(dbi, "iDbi is null");
        this.baseStorageDir = Preconditions.checkNotNull(baseStorageDir, "baseStorageDir is null");
        this.baseStagingDir = Preconditions.checkNotNull(baseStagingDir, "baseStagingDir is null");
        initializeDatabaseIfNecessary();
    }

    @Inject
    public DatabaseStorageManager(@ForStorageManager IDBI dbi)
    {
        this(dbi, DEFAULT_BASE_STORAGE_DIR, DEFAULT_BASE_STAGING_DIR);
    }

    @Override
    public long importTableShard(Operator source, String databaseName, String tableName)
            throws IOException
    {
        // Create a new shard ID for this import attempt
        long shardId = createNewShard(databaseName, tableName);

        // Locally stage the imported data
        FilesAndRowCount filesAndRowCount = stagingImport(source, databaseName, tableName, shardId);

        if (filesAndRowCount.getRowCount() == 0) {
            // This data source is empty, so no work to do here
            return 0;
        }

        // Process staged files to optimize encodings if necessary
        List<File> finalOutputFiles = optimizeEncodings(filesAndRowCount.getFiles(), databaseName, tableName, shardId);

        // Commit all the columns at the same time once everything has been successfully imported
        commitTableShard(shardId, finalOutputFiles);

        return filesAndRowCount.getRowCount();
    }

    private FilesAndRowCount stagingImport(Operator source, String databaseName, String tableName, long shardId)
            throws IOException
    {
        ImmutableList.Builder<File> outputFilesBuilder = ImmutableList.builder();
        ImmutableList.Builder<BlocksFileWriter> writersBuilder = ImmutableList.builder();
        for (int channel = 0; channel < source.getChannelCount(); channel++) {
            File outputFile = new File(createNewFileName(baseStagingDir, databaseName, tableName, shardId, channel, BlocksFileEncoding.RAW));
            Files.createParentDirs(outputFile);

            outputFilesBuilder.add(outputFile);
            writersBuilder.add(new BlocksFileWriter(BlocksFileEncoding.RAW, Files.newOutputStreamSupplier(outputFile)));
        }
        List<File> outputFiles = outputFilesBuilder.build();
        List<BlocksFileWriter> writers = writersBuilder.build();

        long rowCount = ImportingOperator.importData(source, writers);
        return new FilesAndRowCount(outputFiles, rowCount);
    }

    private List<File> optimizeEncodings(List<File> stagedFiles, String databaseName, String tableName, long shardId)
            throws IOException
    {
        ImmutableList.Builder<BlockIterable> sourcesBuilder = ImmutableList.builder();
        ImmutableList.Builder<BlocksFileWriter> writersBuilder = ImmutableList.builder();
        ImmutableList.Builder<File> optimizedFilesBuilder = ImmutableList.builder();

        for (int channel = 0; channel < stagedFiles.size(); channel++) {
            File stagedFile = stagedFiles.get(channel);
            Slice slice = mappedFileCache.getUnchecked(stagedFile.getAbsolutePath());

            // Compute optimal encoding from stats
            BlocksFileReader blocks = BlocksFileReader.readBlocks(slice);
            BlocksFileStats stats = blocks.getStats();
            boolean rleEncode = stats.getAvgRunLength() > RUN_LENGTH_AVERAGE_CUTOFF;
            boolean dicEncode = stats.getUniqueCount() < DICTIONARY_CARDINALITY_CUTOFF;

            // TODO: only need to operate with encodings because want to see names, later we can be smarter when there is a metastore
            BlocksFileEncoding encoding = BlocksFileEncoding.RAW;
//            if (dicEncode && rleEncode) {
//                encoding = BlockSerdes.Encoding.DICTIONARY_RLE;
//            }
//            else if (dicEncode) {
//                encoding = BlockSerdes.Encoding.DICTIONARY_RAW;
//            }
//            else if (rleEncode) {
//                encoding = BlockSerdes.Encoding.RLE;
//            }
//            else {
//                encoding = BlockSerdes.Encoding.RAW;
//            }

            File outputFile = new File(createNewFileName(baseStorageDir, databaseName, tableName, shardId, channel, encoding));
            Files.createParentDirs(outputFile);
            optimizedFilesBuilder.add(outputFile);

            if (encoding == BlocksFileEncoding.RAW) {
                // Should already be raw, so just move
                Files.move(stagedFiles.get(channel), outputFile);
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

    private String createNewFileName(File baseDir, String databaseName, String tableName, long shardId, int fieldIndex, BlocksFileEncoding encoding)
    {
        return baseDir.getAbsolutePath() + "/" + databaseName + "/" + tableName + "/" + fieldIndex + "/" + shardId + "." + encoding.getName() + ".shard";
    }

    private void initializeDatabaseIfNecessary()
    {
        dbi.withHandle(new HandleCallback<Void>()
        {
            @Override
            public Void withHandle(Handle handle)
                    throws Exception
            {
                // TODO: use ids for database and table
                handle.createStatement("CREATE TABLE IF NOT EXISTS shards (shard_id BIGINT PRIMARY KEY AUTO_INCREMENT, database VARCHAR(256), table VARCHAR(256))")
                        .execute();
                return null;
            }
        });
        dbi.withHandle(new HandleCallback<Void>()
        {
            @Override
            public Void withHandle(Handle handle)
                    throws Exception
            {
                // TODO: use generic ids for field_index
                handle.createStatement("CREATE TABLE IF NOT EXISTS columns (shard_id BIGINT, field_index INT, path VARCHAR(512) UNIQUE, PRIMARY KEY(shard_id, field_index), FOREIGN KEY(shard_id) REFERENCES shards(shard_id))")
                        .execute();
                return null;
            }
        });

    }

    private long createNewShard(final String databaseName, final String tableName)
    {
        return dbi.withHandle(new HandleCallback<Long>()
        {
            @Override
            public Long withHandle(Handle handle)
                    throws Exception
            {
                return handle.createStatement("INSERT INTO shards(database, table) values (:database, :table)")
                        .bind("database", databaseName)
                        .bind("table", tableName)
                        .executeAndReturnGeneratedKeys(new ResultSetMapper<Long>()
                        {
                            @Override
                            public Long map(int index, ResultSet r, StatementContext ctx)
                                    throws SQLException
                            {
                                return r.getLong(1);
                            }
                        }).first();
            }
        });
    }

    private void commitTableShard(final long shardId, final List<File> fieldOutputFiles) {
        dbi.withHandle(new HandleCallback<Void>()
        {
            @Override
            public Void withHandle(Handle handle)
                    throws Exception
            {
                return handle.inTransaction(
                        TransactionIsolationLevel.READ_COMMITTED,
                        new TransactionCallback<Void>()
                        {
                            @Override
                            public Void inTransaction(Handle conn, TransactionStatus status)
                                    throws Exception
                            {
                                for (int fieldIndex = 0; fieldIndex < fieldOutputFiles.size(); fieldIndex++) {
                                    conn.createStatement("INSERT INTO columns (shard_id, field_index, path) values (:shard_id, :field_index, :path)")
                                            .bind("shard_id", shardId)
                                            .bind("field_index", fieldIndex)
                                            .bind("path", fieldOutputFiles.get(fieldIndex).getAbsolutePath())
                                            .execute();
                                }
                                return null;
                            }
                        });
            }
        });
    }

    @Override
    public BlockIterable getBlocks(final String databaseName, final String tableName, final int fieldIndex)
    {
        List<File> files = dbi.withHandle(new HandleCallback<List<File>>()
        {
            @Override
            public List<File> withHandle(Handle handle)
                    throws Exception
            {
                return handle.createQuery(
                        "SELECT c.path as path " +
                                "FROM shards s JOIN columns c ON s.shard_id = c.shard_id " +
                                "WHERE s.database = :database " +
                                "AND s.table = :table " +
                                "AND c.field_index = :field_index " +
                                "ORDER BY c.shard_id")
                        .bind("database", databaseName)
                        .bind("table", tableName)
                        .bind("field_index", fieldIndex)
                        .map(new ResultSetMapper<File>()
                        {
                            @Override
                            public File map(int index, ResultSet r, StatementContext ctx)
                                    throws SQLException
                            {
                                return new File(r.getString("path"));
                            }
                        })
                        .list();
            }
        });
        return convertFilesToBlocks(files);
    }

    private BlockIterable convertFilesToBlocks(List<File> files)
     {
         Preconditions.checkArgument(!files.isEmpty(), "no files in stream");

         List<Block> blocks = ImmutableList.copyOf(Iterables.concat(Iterables.transform(files, new Function<File, Iterable<? extends Block>>()
         {
             @Override
             public Iterable<? extends Block> apply(File file)
             {
                 Slice slice = mappedFileCache.getUnchecked(file.getAbsolutePath().replace("/Users/dain/work/fb/presto/", ""));
                 BlocksFileReader blocks = BlocksFileReader.readBlocks(slice);
                 return blocks;
             }
         })));

         return BlockUtils.toBlocks(blocks);
     }
    private static class FilesAndRowCount
    {
        private final List<File> files;
        private final long rowCount;

        private FilesAndRowCount(List<File> files, long rowCount)
        {
            Preconditions.checkNotNull(files, "files is null");
            Preconditions.checkArgument(rowCount >= 0, "rowCount must be at least zero");
            this.files = ImmutableList.copyOf(files);
            this.rowCount = rowCount;
        }

        public List<File> getFiles()
        {
            return files;
        }

        public long getRowCount()
        {
            return rowCount;
        }
    }
}
