package com.facebook.presto.metadata;

import com.facebook.presto.Range;
import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.GenericTupleStream;
import com.facebook.presto.block.SelfDescriptiveSerde;
import com.facebook.presto.block.StatsCollectingTupleStreamSerde;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.facebook.presto.ingest.BlockWriterFactory;
import com.facebook.presto.ingest.ImportingOperator;
import com.facebook.presto.ingest.SerdeBlockWriterFactory;
import com.facebook.presto.nblock.Block;
import com.facebook.presto.nblock.BlockIterable;
import com.facebook.presto.nblock.BlockUtils;
import com.facebook.presto.noperator.AlignmentOperator;
import com.facebook.presto.noperator.Operator;
import com.facebook.presto.serde.BlockSerdes;
import com.facebook.presto.serde.StatsCollectingBlocksSerde;
import com.facebook.presto.serde.StatsCollectingBlocksSerde.StatsCollector.Stats;
import com.facebook.presto.serde.UncompressedBlockSerde;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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

import javax.annotation.Nullable;
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
    private static final StatsCollectingTupleStreamSerde STATS_STAGING_SERDE = new StatsCollectingTupleStreamSerde(new SelfDescriptiveSerde(new UncompressedSerde()));
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

        // Locally stage the imported TupleStream
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

        // todo assure source is closed
        // todo assure source is closed
        // todo assure source is closed
        // todo assure source is closed
        ImmutableList.Builder<File> outputFilesBuilder = ImmutableList.builder();
        ImmutableList.Builder<BlockWriterFactory> writersBuilder = ImmutableList.builder();
        for (int channel = 0; channel < source.getChannelCount(); channel++) {
            File outputFile = new File(createNewFileName(baseStagingDir, databaseName, tableName, shardId, channel, BlockSerdes.Encoding.RAW));
            Files.createParentDirs(outputFile);

            outputFilesBuilder.add(outputFile);
            writersBuilder.add(new SerdeBlockWriterFactory(UncompressedBlockSerde.INSTANCE, Files.newOutputStreamSupplier(outputFile)));
        }
        List<File> outputFiles = outputFilesBuilder.build();
        List<BlockWriterFactory> writers = writersBuilder.build();

        long rowCount = ImportingOperator.importData(source, writers);
        return new FilesAndRowCount(outputFiles, rowCount);
    }

    private List<File> optimizeEncodings(List<File> stagedFiles, String databaseName, String tableName, long shardId)
            throws IOException
    {
        ImmutableList.Builder<BlockIterable> sourcesBuilder = ImmutableList.builder();
        ImmutableList.Builder<BlockWriterFactory> writersBuilder = ImmutableList.builder();
        ImmutableList.Builder<File> optimizedFilesBuilder = ImmutableList.builder();

        for (int channel = 0; channel < stagedFiles.size(); channel++) {
            File stagedFile = stagedFiles.get(channel);
            Slice slice = mappedFileCache.getUnchecked(stagedFile.getAbsolutePath());
            Stats stats = STATS_STAGING_SERDE.createDeserializer().deserializeStats(slice);

            // Compute optimal encoding from stats
            boolean rleEncode = stats.getAvgRunLength() > RUN_LENGTH_AVERAGE_CUTOFF;
            boolean dicEncode = stats.getUniqueCount() < DICTIONARY_CARDINALITY_CUTOFF;

            // TODO: only need to operate with encodings because want to see names, later we can be smarter when there is a metastore
            BlockSerdes.Encoding encoding = BlockSerdes.Encoding.RAW;
//            if (dicEncode && rleEncode) {
//                encoding = TupleStreamSerdes.Encoding.DICTIONARY_RLE;
//            }
//            else if (dicEncode) {
//                encoding = TupleStreamSerdes.Encoding.DICTIONARY_RAW;
//            }
//            else if (rleEncode) {
//                encoding = TupleStreamSerdes.Encoding.RLE;
//            }
//            else {
//                encoding = TupleStreamSerdes.Encoding.RAW;
//            }

            File outputFile = new File(createNewFileName(baseStorageDir, databaseName, tableName, shardId, channel, encoding));
            Files.createParentDirs(outputFile);
            optimizedFilesBuilder.add(outputFile);

            if (encoding == BlockSerdes.Encoding.RAW) {
                // Should already be raw, so just move
                Files.move(stagedFiles.get(channel), outputFile);
            }
            else {
                sourcesBuilder.add(STATS_STAGING_SERDE.createDeserializer().deserializeBlocks(Range.ALL, slice));
                writersBuilder.add(new SerdeBlockWriterFactory(encoding.createSerde(), Files.newOutputStreamSupplier(outputFile)));
            }
        }
        List<BlockIterable> sources = sourcesBuilder.build();
        List<BlockWriterFactory> writers = writersBuilder.build();

        if (!sources.isEmpty()) {
            AlignmentOperator source = new AlignmentOperator(sources);
            ImportingOperator.importData(source, writers);
        }
        return optimizedFilesBuilder.build();
    }

    private String createNewFileName(File baseDir, String databaseName, String tableName, long shardId, int fieldIndex, BlockSerdes.Encoding encoding)
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

    private TupleStream convertFilesToStream(List<File> files)
    {
        files = Lists.transform(files, new Function<File, File>() {
            @Override
            public File apply(@Nullable File input)
            {
                return new File(input.getAbsolutePath().replace("/Users/martint/fb/presto/presto/", ""));
            }
        });
        Preconditions.checkArgument(!files.isEmpty(), "no files in stream");
        final StatsCollectingTupleStreamSerde.StatsAnnotatedTupleStreamDeserializer deserializer = new StatsCollectingTupleStreamSerde.StatsAnnotatedTupleStreamDeserializer(SelfDescriptiveSerde.DESERIALIZER);

        // Use the first file to get the schema
        Slice slice = mappedFileCache.getUnchecked(files.get(0).getAbsolutePath());
        TupleInfo tupleInfo = deserializer.deserialize(Range.ALL, slice).getTupleInfo();

        return new GenericTupleStream<>(tupleInfo, Iterables.transform(files, new Function<File, TupleStream>()
        {
            private Range range;

            @Override
            public TupleStream apply(File file)
            {
                Slice slice = mappedFileCache.getUnchecked(file.getAbsolutePath());
                long rowCount = deserializer.deserializeStats(slice).getRowCount();
                if (range == null) {
                    range = Range.create(0, rowCount);
                } else {
                    range = Range.create(range.getEnd() + 1, range.getEnd() + rowCount);
                }
                TupleStream deserialized = deserializer.deserialize(range, slice);
                return deserialized;
            }
        }));
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

        return BlockUtils.toBlocks(Iterables.concat(Lists.transform(files, new Function<File, Iterable<? extends Block>>()
        {
            private long startPosition;

            @Override
            public Iterable<? extends Block> apply(File file)
            {
                Slice slice = mappedFileCache.getUnchecked(file.getAbsolutePath());
                BlockIterable blocks = StatsCollectingBlocksSerde.readBlocks(slice, startPosition);
                startPosition += StatsCollectingBlocksSerde.readStats(slice).getRowCount() + 1;
                return blocks;
            }
        })));
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
