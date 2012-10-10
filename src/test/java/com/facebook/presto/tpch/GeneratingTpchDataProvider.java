package com.facebook.presto.tpch;

import com.facebook.presto.block.TupleStreamSerializer;
import com.facebook.presto.ingest.BlockDataImporter;
import com.facebook.presto.ingest.BlockExtractor;
import com.facebook.presto.ingest.DelimitedBlockExtractor;
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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

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
            return new InputSupplier<InputStream>() {
                @Override
                public InputStream getInput() throws IOException
                {
                    try {
                        JarFile jarFile = new JarFile(jarFileName);
                        return jarFile.getInputStream(jarFile.getJarEntry(createTableFileName(tableName)));
                    } catch (IOException e) {
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

    private static TableInputSupplierFactory autoSelectTableInputStreamProvider() {
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
            String hash = ByteStreams.hash(tableInputSupplierFactory.getInputSupplier(column.getTableName()), Hashing.md5()).toString();

            File cachedFile = new File(new File(cacheDirectory, column.getTableName() + "-" + hash), createFileName(column, serdeName));
            if (cachedFile.exists()) {
                return cachedFile;
            }

            Files.createParentDirs(cachedFile);

            BlockExtractor blockExtractor = new DelimitedBlockExtractor(
                    Splitter.on('|'),
                    ImmutableList.of(new DelimitedBlockExtractor.ColumnDefinition(column.getIndex(), column.getType()))
            );
            BlockDataImporter importer = new BlockDataImporter(
                    blockExtractor,
                    ImmutableList.of(
                            new BlockDataImporter.ColumnImportSpec(
                                    serializer,
                                    Files.newOutputStreamSupplier(cachedFile)))
            );
            importer.importFrom(
                    new InputSupplier<InputStreamReader>() {
                        @Override
                        public InputStreamReader getInput() throws IOException
                        {
                            return new InputStreamReader(tableInputSupplierFactory.getInputSupplier(column.getTableName()).getInput(), Charsets.UTF_8);
                        }
                    }
            );
            return cachedFile;
        } catch (IOException e) {
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
