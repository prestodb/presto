package com.facebook.presto.tpch;

import com.facebook.presto.block.ProjectionTupleStream;
import com.facebook.presto.block.TupleStreamSerializer;
import com.facebook.presto.ingest.DelimitedRecordIterable;
import com.facebook.presto.ingest.DelimitedTupleStream;
import com.facebook.presto.ingest.ImportingOperator;
import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.ingest.SerdeBlockWriterFactory;
import com.facebook.presto.ingest.StreamWriterTupleValueSink;
import com.facebook.presto.ingest.TupleStreamImporter;
import com.facebook.presto.serde.BlockSerde;
import com.facebook.presto.tpch.TpchSchema.Column;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.jar.JarFile;

import static com.facebook.presto.ingest.RecordProjections.createProjection;
import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.io.CharStreams.newReaderSupplier;
import static com.google.common.io.Files.newOutputStreamSupplier;

/**
 * Extracts TPCH data into serialized column file formats.
 * It will also cache the extracted columns in the local file system to help mitigate the cost of the operation.
 */
public class GeneratingTpchDataProvider
        implements TpchDataProvider
{
    private final TableInputSupplierFactory tableInputSupplierFactory;
    private final File cacheDirectory;

    public GeneratingTpchDataProvider(TableInputSupplierFactory tableInputSupplierFactory, File cacheDirectory)
    {
        checkNotNull(tableInputSupplierFactory, "tableInputStreamProvider is null");
        checkNotNull(cacheDirectory, "cacheDirectory is null");
        checkArgument(!cacheDirectory.exists() || cacheDirectory.isDirectory(), "cacheDirectory must be a directory");
        this.tableInputSupplierFactory = tableInputSupplierFactory;
        this.cacheDirectory = cacheDirectory;
    }

    public GeneratingTpchDataProvider(TableInputSupplierFactory tableInputSupplierFactory, String cacheDirectoryName)
    {
        this(tableInputSupplierFactory, new File(checkNotNull(cacheDirectoryName, "cacheDirectoryName is null")));
    }

    public GeneratingTpchDataProvider(String cacheDirectoryName)
    {
        this(autoSelectTableInputStreamProvider(), cacheDirectoryName);
    }

    public GeneratingTpchDataProvider()
    {
        this(System.getProperty("tpchCacheDir", "/tmp/tpchdatacache"));
    }

    private interface TableInputSupplierFactory
    {
        InputSupplier<InputStream> getInputSupplier(String tableName);
    }

    private static class JarTableInputSupplierFactory
            implements TableInputSupplierFactory
    {
        private final String jarFileName;

        private JarTableInputSupplierFactory(String jarFileName)
        {
            this.jarFileName = checkNotNull(jarFileName, "jarFileName is null");
        }

        @Override
        public InputSupplier<InputStream> getInputSupplier(final String tableName)
        {
            checkNotNull(tableName, "tableFileName is null");
            return new InputSupplier<InputStream>()
            {
                @Override
                public InputStream getInput()
                        throws IOException
                {
                    try {
                        JarFile jarFile = new JarFile(jarFileName);
                        return jarFile.getInputStream(jarFile.getJarEntry(createTableFileName(tableName)));
                    }
                    catch (IOException e) {
                        throw Throwables.propagate(e);
                    }
                }
            };
        }
    }

    private static class ResourcesTableInputSupplierFactory
            implements TableInputSupplierFactory
    {
        @Override
        public InputSupplier<InputStream> getInputSupplier(String tableName)
        {
            checkNotNull(tableName, "tableFileName is null");
            return Resources.newInputStreamSupplier(Resources.getResource(createTableFileName(tableName)));
        }
    }

    private static TableInputSupplierFactory autoSelectTableInputStreamProvider()
    {
        // First check if a data jar file has been manually specified
        final String tpchDataJarFileOverride = System.getProperty("tpchDataJar");
        if (tpchDataJarFileOverride != null) {
            return new JarTableInputSupplierFactory(tpchDataJarFileOverride);
        }
        // Otherwise fall back to the default in resources if one is available
        else {
            return new ResourcesTableInputSupplierFactory();
        }
    }

    // TODO: make this work for columns with more than one file
    @Override
    public File getColumnFile(final TpchSchema.Column column, TupleStreamSerializer serializer, String serdeName)
    {
        checkNotNull(column, "column is null");
        checkNotNull(serializer, "serializer is null");
        checkNotNull(serdeName, "serdeName is null");

        try {
            String hash = ByteStreams.hash(tableInputSupplierFactory.getInputSupplier(column.getTable().getName()), Hashing.md5()).toString();

            File cachedFile = new File(new File(cacheDirectory, column.getTable().getName() + "-" + hash), createFileName(column, serdeName));
            if (cachedFile.exists()) {
                return cachedFile;
            }

            Files.createParentDirs(cachedFile);

            try (InputStream input = tableInputSupplierFactory.getInputSupplier(column.getTable().getName()).getInput()) {
                TupleStreamImporter.importFrom(
                        new ProjectionTupleStream(
                                new DelimitedTupleStream(
                                        new InputStreamReader(input, Charsets.UTF_8),
                                        Splitter.on("|"),
                                        column.getTable().getTupleInfo()
                                ),
                                column.getIndex()
                        ),
                        ImmutableList.of(new StreamWriterTupleValueSink(Files.newOutputStreamSupplier(cachedFile), serializer))
                );
            }

            return cachedFile;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public File getColumnFile(Column column, BlockSerde blockSerde, String serdeName)
    {
        checkNotNull(column, "column is null");
        checkNotNull(blockSerde, "blockSerde is null");
        checkNotNull(serdeName, "serdeName is null");

        try {
            String hash = ByteStreams.hash(tableInputSupplierFactory.getInputSupplier(column.getTable().getName()), Hashing.md5()).toString();

            File cachedFile = new File(new File(cacheDirectory, column.getTable().getName() + "-" + hash), "new-" + createFileName(column, serdeName));
            if (cachedFile.exists()) {
                return cachedFile;
            }

            Files.createParentDirs(cachedFile);

            InputSupplier<InputStream> inputSupplier = tableInputSupplierFactory.getInputSupplier(column.getTable().getName());

            DelimitedRecordIterable records = new DelimitedRecordIterable(
                    newReaderSupplier(inputSupplier, UTF_8),
                    Splitter.on("|")
            );
            RecordProjectOperator source = new RecordProjectOperator(records, createProjection(column.getIndex(), column.getType()));

            ImportingOperator.importData(source, new SerdeBlockWriterFactory(blockSerde, newOutputStreamSupplier(cachedFile)));

            return cachedFile;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static String createTableFileName(String tableName)
    {
        return tableName + ".tbl";
    }

    private static String createFileName(TpchSchema.Column column, String serdeName)
    {
        return String.format("column%d.%s_%s.data", column.getIndex(), column.getType().getName(), serdeName);
    }
}
