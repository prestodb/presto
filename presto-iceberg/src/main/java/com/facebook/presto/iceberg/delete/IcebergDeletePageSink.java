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

import com.facebook.airlift.json.JsonCodec;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.RunLengthEncodedBlock;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.iceberg.CommitTaskData;
import com.facebook.presto.iceberg.FileFormat;
import com.facebook.presto.iceberg.IcebergFileWriter;
import com.facebook.presto.iceberg.IcebergFileWriterFactory;
import com.facebook.presto.iceberg.MetricsWrapper;
import com.facebook.presto.iceberg.PartitionData;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.iceberg.MetricsConfig;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.io.LocationProvider;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.common.Utils.nativeValueToBlock;
import static com.facebook.presto.hive.util.ConfigurationUtils.toJobConf;
import static com.facebook.presto.iceberg.FileContent.POSITION_DELETES;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_BAD_DATA;
import static com.facebook.presto.iceberg.IcebergErrorCode.ICEBERG_ROLLBACK_ERROR;
import static com.facebook.presto.iceberg.IcebergUtil.partitionDataFromJson;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_PATH;
import static org.apache.iceberg.MetadataColumns.DELETE_FILE_POS;
import static org.apache.iceberg.TableProperties.DEFAULT_WRITE_METRICS_MODE;

public class IcebergDeletePageSink
        implements ConnectorPageSink
{
    private final PartitionSpec partitionSpec;
    private final Optional<PartitionData> partitionData;
    private final IcebergFileWriterFactory fileWriterFactory;
    private final HdfsEnvironment hdfsEnvironment;
    private final HdfsContext hdfsContext;
    private final JobConf jobConf;
    private final JsonCodec<CommitTaskData> jsonCodec;
    private final ConnectorSession session;
    private final String dataFile;
    private final FileFormat fileFormat;
    private Path outputPath;

    private final IcebergPositionDeleteWriter positionDeleteWriter;

    private long writtenBytes;
    private long validationCpuNanos;
    private static final MetricsConfig FULL_METRICS_CONFIG = MetricsConfig.fromProperties(ImmutableMap.of(DEFAULT_WRITE_METRICS_MODE, "full"));

    public IcebergDeletePageSink(
            PartitionSpec partitionSpec,
            Optional<String> partitionDataAsJson,
            LocationProvider locationProvider,
            IcebergFileWriterFactory fileWriterFactory,
            HdfsEnvironment hdfsEnvironment,
            HdfsContext hdfsContext,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session,
            String dataFile,
            FileFormat fileFormat)
    {
        this.fileWriterFactory = requireNonNull(fileWriterFactory, "fileWriterFactory is null");
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.hdfsContext = requireNonNull(hdfsContext, "hdfsContext is null");
        this.jobConf = toJobConf(hdfsEnvironment.getConfiguration(hdfsContext, new Path(locationProvider.newDataLocation("delete-file"))));
        this.jsonCodec = requireNonNull(jsonCodec, "jsonCodec is null");
        this.session = requireNonNull(session, "session is null");
        this.dataFile = requireNonNull(dataFile, "dataFile is null");
        this.fileFormat = requireNonNull(fileFormat, "fileFormat is null");
        this.partitionSpec = requireNonNull(partitionSpec, "partitionSpec is null");
        this.partitionData = partitionDataFromJson(partitionSpec, partitionDataAsJson);
        String fileName = fileFormat.addExtension(String.format("delete_file_%s", randomUUID().toString()));
        this.outputPath = partitionData.map(partition -> new Path(locationProvider.newDataLocation(partitionSpec, partition, fileName)))
                .orElseGet(() -> new Path(locationProvider.newDataLocation(fileName)));
        this.positionDeleteWriter = new IcebergPositionDeleteWriter();
    }

    @Override
    public long getCompletedBytes()
    {
        return writtenBytes;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return positionDeleteWriter.getSystemMemoryUsage();
    }

    @Override
    public long getValidationCpuNanos()
    {
        return validationCpuNanos;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        positionDeleteWriter.appendPage(page);
        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        ImmutableList.Builder<Slice> commitTasks = ImmutableList.builder();

        IcebergFileWriter writer = positionDeleteWriter.getWriter();

        writer.commit();

        CommitTaskData task = new CommitTaskData(
                outputPath.toString(),
                writer.getFileSizeInBytes(),
                new MetricsWrapper(writer.getMetrics()),
                partitionSpec.specId(),
                partitionData.map(PartitionData::toJson),
                fileFormat,
                dataFile,
                POSITION_DELETES);

        commitTasks.add(wrappedBuffer(jsonCodec.toJsonBytes(task)));

        writtenBytes = writer.getWrittenBytes();
        validationCpuNanos = writer.getValidationCpuNanos();

        return completedFuture(commitTasks.build());
    }

    @Override
    public void abort()
    {
        try {
            positionDeleteWriter.getWriter().rollback();
        }
        catch (Throwable t) {
            throw new PrestoException(ICEBERG_ROLLBACK_ERROR, "Exception during rollback");
        }
    }

    private class IcebergPositionDeleteWriter
    {
        private final Schema positionDeleteSchema = new Schema(DELETE_FILE_PATH, DELETE_FILE_POS);
        private final IcebergFileWriter writer;

        public IcebergPositionDeleteWriter()
        {
            this.writer = createWriter();
        }

        public void appendPage(Page page)
        {
            if (page.getChannelCount() == 1) {
                hdfsEnvironment.doAs(session.getUser(), () -> {
                    Block[] blocks = new Block[2];
                    blocks[0] = new RunLengthEncodedBlock(nativeValueToBlock(VarcharType.VARCHAR, utf8Slice(dataFile)), page.getPositionCount());
                    blocks[1] = page.getBlock(0);
                    writer.appendRows(new Page(blocks));
                });
            }
            else {
                throw new PrestoException(ICEBERG_BAD_DATA, "Expecting Page with one channel but got " + page.getChannelCount());
            }
        }

        public IcebergFileWriter getWriter()
        {
            return writer;
        }

        public long getSystemMemoryUsage()
        {
            return writer.getSystemMemoryUsage();
        }

        private IcebergFileWriter createWriter()
        {
            return fileWriterFactory.createFileWriter(
                    outputPath,
                    positionDeleteSchema,
                    jobConf,
                    session,
                    hdfsContext,
                    fileFormat,
                    FULL_METRICS_CONFIG);
        }
    }
}
