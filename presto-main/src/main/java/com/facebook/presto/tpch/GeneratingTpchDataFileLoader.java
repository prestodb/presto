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
package com.facebook.presto.tpch;

import com.facebook.presto.execution.TaskId;
import com.facebook.presto.metadata.ColumnFileHandle;
import com.facebook.presto.operator.OperatorContext;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.RecordProjectOperator;
import com.facebook.presto.operator.TaskContext;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.util.DelimitedRecordSet;
import com.facebook.presto.util.Threads;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.hash.Hashing;
import com.google.common.io.ByteStreams;
import com.google.common.io.Files;
import com.google.common.io.InputSupplier;
import com.google.common.io.Resources;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.jar.JarFile;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.io.CharStreams.newReaderSupplier;

/**
 * Extracts TPCH data into serialized column file formats.
 * It will also cache the extracted columns in the local file system to help mitigate the cost of the operation.
 */
public class GeneratingTpchDataFileLoader
        implements TpchDataFileLoader
{
    private final TableInputSupplierFactory tableInputSupplierFactory;
    private final File cacheDirectory;

    public GeneratingTpchDataFileLoader(TableInputSupplierFactory tableInputSupplierFactory, File cacheDirectory)
    {
        checkNotNull(tableInputSupplierFactory, "tableInputStreamProvider is null");
        checkNotNull(cacheDirectory, "cacheDirectory is null");
        checkArgument(!cacheDirectory.exists() || cacheDirectory.isDirectory(), "cacheDirectory must be a directory");
        this.tableInputSupplierFactory = tableInputSupplierFactory;
        this.cacheDirectory = cacheDirectory;
    }

    public GeneratingTpchDataFileLoader(TableInputSupplierFactory tableInputSupplierFactory, String cacheDirectoryName)
    {
        this(tableInputSupplierFactory, new File(checkNotNull(cacheDirectoryName, "cacheDirectoryName is null")));
    }

    public GeneratingTpchDataFileLoader(String cacheDirectoryName)
    {
        this(autoSelectTableInputStreamProvider(), cacheDirectoryName);
    }

    public GeneratingTpchDataFileLoader()
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

    @Override
    public File getDataFile(TpchTableHandle tableHandle, TpchColumnHandle columnHandle, BlocksFileEncoding encoding)
    {
        checkNotNull(tableHandle, "tableHandle is null");
        checkNotNull(columnHandle, "columnHandle is null");
        checkNotNull(encoding, "encoding is null");

        String tableName = tableHandle.getTableName();
        ExecutorService executor = Executors.newCachedThreadPool(Threads.daemonThreadsNamed("tpch-generate-%s"));
        try {
            String hash = ByteStreams.hash(ByteStreams.slice(tableInputSupplierFactory.getInputSupplier(tableName), 0, 1024 * 1024), Hashing.murmur3_32()).toString();

            File cachedFile = new File(new File(cacheDirectory, tableName + "-" + hash), "new-" + createFileName(columnHandle, encoding));
            if (cachedFile.exists()) {
                return cachedFile;
            }

            Files.createParentDirs(cachedFile);

            InputSupplier<InputStream> inputSupplier = tableInputSupplierFactory.getInputSupplier(tableName);

            ColumnMetadata columnMetadata = new TpchMetadata().getColumnMetadata(tableHandle, columnHandle);

            DelimitedRecordSet records = new DelimitedRecordSet(newReaderSupplier(inputSupplier, UTF_8), Splitter.on("|"), columnMetadata);

            Session session = new Session("user", "source", "catalog", "schema", "address", "agent");
            OperatorContext operatorContext = new TaskContext(new TaskId("query", "stage", "task"), executor, session)
                    .addPipelineContext(true, true)
                    .addDriverContext()
                    .addOperatorContext(0, "tpch-generate");

            RecordProjectOperator source = new RecordProjectOperator(operatorContext, records);

            ColumnFileHandle columnFileHandle = ColumnFileHandle.builder(UUID.randomUUID())
                    .addColumn(columnHandle, cachedFile, encoding)
                    .build();

            while (!source.isFinished()) {
                Page page = source.getOutput();
                if (page != null) {
                    columnFileHandle.append(page);
                }
            }
            columnFileHandle.commit();

            return cachedFile;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        finally {
            executor.shutdownNow();
        }
    }

    private static String createTableFileName(String tableName)
    {
        return tableName + ".tbl";
    }

    private static String createFileName(TpchColumnHandle columnHandle, BlocksFileEncoding encoding)
    {
        return String.format("column%d.%s_%s.data", columnHandle.getFieldIndex(), columnHandle.getType(), encoding.getName());
    }
}
