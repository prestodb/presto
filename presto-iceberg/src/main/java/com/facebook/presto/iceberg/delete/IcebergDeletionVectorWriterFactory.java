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
package com.facebook.presto.iceberg.delete;

import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.HdfsInputFile;
import com.facebook.presto.iceberg.HdfsOutputFile;
import com.facebook.presto.iceberg.PrestoIcebergTableForMetricsConfig;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.encryption.EncryptionManager;
import org.apache.iceberg.encryption.PlaintextEncryptionManager;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.io.OutputFileFactory;

import java.util.Map;
import java.util.Optional;

import static com.google.common.collect.ImmutableMap.of;
import static java.util.Objects.requireNonNull;

/**
 * Factory for building an {@link OutputFileFactory} configured for writing Puffin
 * deletion-vector (DV) files via Iceberg's native {@link org.apache.iceberg.deletes.BaseDVFileWriter}.
 *
 * <p>The Iceberg builder requires a {@link org.apache.iceberg.Table} to read the
 * partition spec, encryption manager and location provider.  Presto's page-sink path
 * does not have a fully-loaded {@code Table} available, so this factory wraps the
 * primitives Presto already has (HDFS environment, location provider, partition
 * spec) into a minimal {@link org.apache.iceberg.Table} implementation that exposes
 * just what {@link OutputFileFactory.Builder} needs.
 */
public final class IcebergDeletionVectorWriterFactory
{
    private IcebergDeletionVectorWriterFactory() {}

    /**
     * Builds an {@link OutputFileFactory} that writes Puffin DV files via Presto's
     * HDFS plumbing (which already wraps {@code doAs} per session user).
     *
     * @param hdfsEnvironment Presto HDFS environment (provides {@code doAs}).
     * @param hdfsContext     Per-query HDFS context (carries the user identity).
     * @param locationProvider Iceberg location provider for the table.
     * @param partitionSpec   Partition spec the produced DV files belong to.
     * @param operationId     Stable identifier (e.g. session/query id) embedded
     *                        in generated file names.
     * @param partitionId     Logical partition id used in generated file names.
     * @param taskId          Logical task id used in generated file names.
     */
    public static OutputFileFactory createPuffinOutputFileFactory(
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            LocationProvider locationProvider,
            PartitionSpec partitionSpec,
            String operationId,
            int partitionId,
            long taskId)
    {
        requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        requireNonNull(hdfsContext, "hdfsContext is null");
        requireNonNull(locationProvider, "locationProvider is null");
        requireNonNull(partitionSpec, "partitionSpec is null");
        requireNonNull(operationId, "operationId is null");

        FileIO io = new HdfsBackedFileIO(hdfsEnvironment, hdfsContext);
        org.apache.iceberg.Table table = new MinimalDvTable(partitionSpec, locationProvider, io);
        return OutputFileFactory.builderFor(table, partitionId, taskId)
                .format(org.apache.iceberg.FileFormat.PUFFIN)
                .operationId(operationId)
                .ioSupplier(() -> io)
                .build();
    }

    /**
     * Lightweight {@link FileIO} implementation that delegates to Presto's
     * {@link HdfsOutputFile} / {@link HdfsInputFile}.  Avoids pulling in the
     * {@code ManifestFileCache} dependency that the production
     * {@code com.facebook.presto.iceberg.HdfsFileIO} requires.
     */
    private static final class HdfsBackedFileIO
            implements FileIO
    {
        private final HdfsEnvironment hdfsEnvironment;
        private final HdfsContext hdfsContext;

        HdfsBackedFileIO(HdfsEnvironment hdfsEnvironment, HdfsContext hdfsContext)
        {
            this.hdfsEnvironment = hdfsEnvironment;
            this.hdfsContext = hdfsContext;
        }

        @Override
        public InputFile newInputFile(String path)
        {
            return new HdfsInputFile(new Path(path), hdfsEnvironment, hdfsContext);
        }

        @Override
        public OutputFile newOutputFile(String path)
        {
            return new HdfsOutputFile(new Path(path), hdfsEnvironment, hdfsContext);
        }

        @Override
        public void deleteFile(String path)
        {
            throw new UnsupportedOperationException("HdfsBackedFileIO does not support deleteFile");
        }
    }

    /**
     * Minimal {@link org.apache.iceberg.Table} that exposes only the fields
     * {@link OutputFileFactory.Builder} reads at build time
     * ({@link #spec()}, {@link #locationProvider()}, {@link #encryption()})
     * plus {@link #io()} as a fallback (overridden via {@code ioSupplier}).
     */
    private static final class MinimalDvTable
            extends PrestoIcebergTableForMetricsConfig
    {
        private static final Map<String, String> EMPTY_PROPERTIES = of();
        private static final Schema EMPTY_SCHEMA = new Schema();
        private static final EncryptionManager PLAINTEXT_ENCRYPTION = PlaintextEncryptionManager.instance();

        private final LocationProvider locationProvider;
        private final FileIO io;

        MinimalDvTable(PartitionSpec spec, LocationProvider locationProvider, FileIO io)
        {
            super(EMPTY_SCHEMA, spec, EMPTY_PROPERTIES, Optional.empty());
            this.locationProvider = locationProvider;
            this.io = io;
        }

        @Override
        public LocationProvider locationProvider()
        {
            return locationProvider;
        }

        @Override
        public EncryptionManager encryption()
        {
            return PLAINTEXT_ENCRYPTION;
        }

        // Defense-in-depth: also expose the FileIO via Table.io() so that any future
        // Iceberg upgrade that bypasses OutputFileFactory.Builder.ioSupplier(...) and
        // calls table.io() directly still routes through Presto's HDFS plumbing.
        @Override
        public FileIO io()
        {
            return io;
        }
    }
}
