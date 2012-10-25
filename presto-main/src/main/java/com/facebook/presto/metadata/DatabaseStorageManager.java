package com.facebook.presto.metadata;

import com.facebook.presto.TupleInfo;
import com.facebook.presto.block.GenericTupleStream;
import com.facebook.presto.block.RepositioningTupleStream;
import com.facebook.presto.block.SelfDescriptiveSerde;
import com.facebook.presto.block.StatsCollectingTupleStreamSerde;
import com.facebook.presto.block.TupleStream;
import com.facebook.presto.block.TupleStreamDeserializer;
import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.block.uncompressed.UncompressedSerde;
import com.facebook.presto.ingest.StreamWriterTupleValueSink;
import com.facebook.presto.ingest.TupleStreamImporter;
import com.facebook.presto.operator.MergeOperator;
import com.facebook.presto.operator.tap.StatsTupleValueSink;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.slice.Slices;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
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
    private static final StatsCollectingTupleStreamSerde STATS_STAGING_SERDE = new StatsCollectingTupleStreamSerde(new SelfDescriptiveSerde(new UncompressedSerde()));
    private static final int RUN_LENGTH_AVERAGE_CUTOFF = 3;
    private static final int DICTIONARY_CARDINALITY_CUTOFF = 1000;

    private final IDBI dbi;
    private final File baseStorageDir;
    private final File baseStagingDir;

    public DatabaseStorageManager(IDBI dbi, File baseStorageDir, File baseStagingDir)
    {
        this.dbi = Preconditions.checkNotNull(dbi, "iDbi is null");
        this.baseStorageDir = Preconditions.checkNotNull(baseStorageDir, "baseStorageDir is null");
        this.baseStagingDir = Preconditions.checkNotNull(baseStagingDir, "baseStagingDir is null");
        initializeDatabaseIfNecessary();
    }

    @Inject
    public DatabaseStorageManager(IDBI dbi)
    {
        this(dbi, DEFAULT_BASE_STORAGE_DIR, DEFAULT_BASE_STAGING_DIR);
    }

    @Override
    public long importTableShard(TupleStream sourceTupleStream, String databaseName, String tableName)
            throws IOException
    {
        // Create a new shard ID for this import attempt
        long shardId = createNewShard(databaseName, tableName);

        // Locally stage the imported TupleStream
        FilesAndRowCount filesAndRowCount = stagingImport(sourceTupleStream, databaseName, tableName, shardId);

        if (filesAndRowCount.getRowCount() == 0) {
            // This data source is empty, so no work to do here
            return 0;
        }

        // Process staged files to optimize encodings if necessary
        List<File> finalOutputFiles = optimizeEncodings(filesAndRowCount.getFiles(), sourceTupleStream.getTupleInfo().getFieldCount(), databaseName, tableName, shardId);

        // Commit all the columns at the same time once everything has been successfully imported
        commitTableShard(shardId, finalOutputFiles);

        return filesAndRowCount.getRowCount();
    }

    private FilesAndRowCount stagingImport(TupleStream sourceTupleStream, String databaseName, String tableName, long shardId)
            throws IOException
    {
        ImmutableList.Builder<File> rawFilesBuilder = ImmutableList.builder();
        ImmutableList.Builder<StreamWriterTupleValueSink> tupleValueSinkBuilder = ImmutableList.builder();
        for (int field = 0; field < sourceTupleStream.getTupleInfo().getFieldCount(); field++) {
            File outputFile = new File(createNewFileName(baseStagingDir, databaseName, tableName, shardId, field, TupleStreamSerdes.Encoding.RAW));
            Files.createParentDirs(outputFile);
            rawFilesBuilder.add(outputFile);

            tupleValueSinkBuilder.add(
                    new StreamWriterTupleValueSink(
                            Files.newOutputStreamSupplier(outputFile),
                            STATS_STAGING_SERDE.createSerializer()
                    )
            );
        }
        long rowCount = TupleStreamImporter.importFrom(sourceTupleStream, tupleValueSinkBuilder.build());
        return new FilesAndRowCount(rawFilesBuilder.build(), rowCount);
    }

    private List<File> optimizeEncodings(List<File> stagedFiles, int fieldCount, String databaseName, String tableName, long shardId)
            throws IOException
    {
        Preconditions.checkArgument(stagedFiles.size() == fieldCount, "field count does not match provided files");
        ImmutableList.Builder<StreamWriterTupleValueSink> tupleValueSinkBuilder = ImmutableList.builder();
        ImmutableList.Builder<TupleStream> sourceStreamsBuilder = ImmutableList.builder();
        ImmutableList.Builder<File> optimizedFilesBuilder = ImmutableList.builder();
        for (int field = 0; field < fieldCount; field++) {
            Slice slice = Slices.mapFileReadOnly(stagedFiles.get(field));
            StatsTupleValueSink.Stats stats = STATS_STAGING_SERDE.createDeserializer().deserializeStats(slice);

            // Compute optimal encoding from stats
            boolean rleEncode = stats.getAvgRunLength() > RUN_LENGTH_AVERAGE_CUTOFF;
            boolean dicEncode = stats.getUniqueCount() < DICTIONARY_CARDINALITY_CUTOFF;

            // TODO: only need to operate with encodings because want to see names, later we can be smarter when there is a metastore
            TupleStreamSerdes.Encoding encoding;
            if (dicEncode && rleEncode) {
                encoding = TupleStreamSerdes.Encoding.DICTIONARY_RLE;
            }
            else if (dicEncode) {
                encoding = TupleStreamSerdes.Encoding.DICTIONARY_RAW;
            }
            else if (rleEncode) {
                encoding = TupleStreamSerdes.Encoding.RLE;
            }
            else {
                encoding = TupleStreamSerdes.Encoding.RAW;
            }

            File outputFile = new File(createNewFileName(baseStorageDir, databaseName, tableName, shardId, field, encoding));
            optimizedFilesBuilder.add(outputFile);
            Files.createParentDirs(outputFile);
            if (encoding == TupleStreamSerdes.Encoding.RAW) {
                // Should already be raw, so just move
                Files.move(stagedFiles.get(field), outputFile);
            }
            else {
                sourceStreamsBuilder.add(STATS_STAGING_SERDE.createDeserializer().deserialize(slice));
                tupleValueSinkBuilder.add(
                        new StreamWriterTupleValueSink(
                                Files.newOutputStreamSupplier(outputFile),
                                new StatsCollectingTupleStreamSerde(new SelfDescriptiveSerde(encoding.createSerde())).createSerializer()
                        )
                );
            }
        }
        List<TupleStream> sourceTupleStreams = sourceStreamsBuilder.build();

        if (!sourceTupleStreams.isEmpty()) {
            TupleStreamImporter.importFrom(new MergeOperator(sourceTupleStreams), tupleValueSinkBuilder.build());
        }
        return optimizedFilesBuilder.build();
    }

    private String createNewFileName(File baseDir, String databaseName, String tableName, long shardId, int fieldIndex, TupleStreamSerdes.Encoding encoding)
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
    public TupleStream getTupleStream(final String databaseName, final String tableName, final int fieldIndex)
    {
        return convertFilesToStream(dbi.withHandle(new HandleCallback<List<File>>()
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
        }));
    }

    private TupleStream convertFilesToStream(List<File> files)
    {
        Preconditions.checkArgument(!files.isEmpty(), "no files in stream");
        final StatsCollectingTupleStreamSerde.StatsAnnotatedTupleStreamDeserializer deserializer = new StatsCollectingTupleStreamSerde.StatsAnnotatedTupleStreamDeserializer(SelfDescriptiveSerde.DESERIALIZER);
        try {
            // Use the first file to get the schema
            TupleInfo tupleInfo = deserializer.deserialize(Slices.mapFileReadOnly(files.get(0))).getTupleInfo();

            return new GenericTupleStream<>(tupleInfo, Iterables.transform(files, new Function<File, TupleStream>()
            {
                private long nextStartingPosition = 0;

                @Override
                public TupleStream apply(File file)
                {
                    try {
                        Slice slice = Slices.mapFileReadOnly(file);
                        RepositioningTupleStream tupleStream = new RepositioningTupleStream(
                                deserializer.deserialize(slice),
                                nextStartingPosition
                        );
                        nextStartingPosition += deserializer.deserializeStats(slice).getRowCount();
                        return tupleStream;
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
            }));
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
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
