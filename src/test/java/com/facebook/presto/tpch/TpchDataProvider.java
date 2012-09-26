package com.facebook.presto.tpch;

import com.facebook.presto.block.TupleStreamSerdes;
import com.facebook.presto.ingest.BlockDataImporter;
import com.facebook.presto.ingest.BlockExtractor;
import com.facebook.presto.ingest.DelimitedBlockExtractor;
import com.facebook.presto.tpch.TpchSchema;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TpchDataProvider
{
    private final File cacheDirectory;

    public TpchDataProvider(File cacheDirectory)
    {
        checkNotNull(cacheDirectory, "cacheDirectory is null");
        checkArgument(!cacheDirectory.exists() || cacheDirectory.isDirectory(), "cacheDirectory must be a directory");
        this.cacheDirectory = cacheDirectory;
    }

    public TpchDataProvider(String cacheDirectoryName)
    {
        this(new File(checkNotNull(cacheDirectoryName, "cacheDirectoryName is null")));
    }

    public TpchDataProvider()
    {
        this(System.getProperty("tcphCacheDir", "/tmp/tcphdatacache"));
    }

    // TODO: make this work for columns with more than one file
    public File getColumnFile(TpchSchema.Column column, TupleStreamSerdes.Encoding encoding) throws IOException
    {
        checkNotNull(column, "column is null");
        checkNotNull(encoding, "encoding is null");

        URL sourceTableUrl = Resources.getResource(column.getTableName() + ".tbl");
        String hash = ByteStreams.hash(Resources.newInputStreamSupplier(sourceTableUrl), Hashing.sha1()).toString();

        File cachedFile = new File(new File(cacheDirectory, column.getTableName() + "-" + hash), createFileName(column, encoding));
        if (cachedFile.exists()) {
            return cachedFile;
        }

        Files.createParentDirs(cachedFile);

        InputSupplier<InputStreamReader> inputSupplier = Resources.newReaderSupplier(sourceTableUrl, Charsets.UTF_8);
        BlockExtractor blockExtractor = new DelimitedBlockExtractor(
                ImmutableList.of(new DelimitedBlockExtractor.ColumnDefinition(column.getIndex(), column.getType())),
                Splitter.on('|')
        );
        BlockDataImporter importer = new BlockDataImporter(
                blockExtractor,
                ImmutableList.of(
                        new BlockDataImporter.ColumnImportSpec(
                                TupleStreamSerdes.createTupleStreamSerde(encoding),
                                Files.newOutputStreamSupplier(cachedFile)))
        );
        importer.importFrom(inputSupplier);
        return cachedFile;
    }

    private static String createFileName(TpchSchema.Column column, TupleStreamSerdes.Encoding encoding)
    {
        return String.format("column%d.%s_%s.data", column.getIndex(), column.getType().getName(), encoding.getName());
    }
}
