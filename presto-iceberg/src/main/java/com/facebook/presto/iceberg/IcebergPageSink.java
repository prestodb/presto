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
package com.facebook.presto.iceberg;

import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.HiveWriteUtils;
import com.facebook.presto.iceberg.parquet.writer.PrestoWriteSupport;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Joiner;
import com.google.common.primitives.Ints;
import com.netflix.iceberg.FileFormat;
import com.netflix.iceberg.Metrics;
import com.netflix.iceberg.Schema;
import com.netflix.iceberg.exceptions.RuntimeIOException;
import com.netflix.iceberg.hadoop.HadoopOutputFile;
import com.netflix.iceberg.io.FileAppender;
import com.netflix.iceberg.io.OutputFile;
import com.netflix.iceberg.parquet.Parquet;
import com.netflix.iceberg.parquet.TypeToMessageType;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.iceberg.MetricsParser.toJson;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;

public class IcebergPageSink
        implements ConnectorPageSink
{
    private Schema outputSchema;
    private String outputDir;
    private Configuration configuration;
    private List<HiveColumnHandle> inputColumns;
    private List<HiveColumnHandle> partitionColumns;
    private Map<String, PartitionWriteContext> partitionToWriterContext;
    private Map<String, String> partitionToFile;
    private JsonCodec<CommitTaskData> jsonCodec;
    private final ConnectorSession session;
    private final TypeManager typeManager;
    private final FileFormat fileFormat;
    final List<Type> partitionTypes;
    private static final String PATH_SEPERATOR = "/";
    private static final Joiner PATH_JOINER = Joiner.on(PATH_SEPERATOR);
    private static final TypeToMessageType typeToMessageType = new TypeToMessageType();

    public IcebergPageSink(Schema outputSchema,
            String outputDir,
            Configuration configuration,
            List<HiveColumnHandle> inputColumns,
            TypeManager typeManager,
            JsonCodec<CommitTaskData> jsonCodec,
            ConnectorSession session, FileFormat fileFormat)
    {
        this.outputSchema = outputSchema;
        this.outputDir = outputDir;
        this.configuration = configuration;
        // TODO we only mark identity columns as partition columns as of now but we need to extract the schema on the coordinator side and provide partition
        // transforms in ConnectorOutputTableHandle
        this.partitionColumns = inputColumns.stream().filter(col -> col.isPartitionKey()).collect(toList());
        this.jsonCodec = jsonCodec;
        this.session = session;
        this.typeManager = typeManager;
        this.fileFormat = fileFormat;
        this.partitionToWriterContext = new HashMap<>();
        this.partitionToFile = new HashMap<>();
        this.partitionTypes = partitionColumns.stream().map(col -> typeManager.getType(col.getTypeSignature())).collect(toList());
        this.inputColumns = inputColumns;
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        final int numRows = page.getPositionCount();

        for (int rowNum = 0; rowNum < numRows; rowNum++) {
            final String partitionPath = getPartitionPath(page, rowNum);

            if (!partitionToWriterContext.containsKey(partitionPath)) {
                partitionToWriterContext.put(partitionPath, new PartitionWriteContext(new ArrayList<>(), addWriter(partitionPath)));
            }
            partitionToWriterContext.get(partitionPath).getRowNum().add(rowNum);
        }

        for (Map.Entry<String, PartitionWriteContext> partitionToWriter : partitionToWriterContext.entrySet()) {
            final List<Integer> rowNums = partitionToWriter.getValue().getRowNum();
            final FileAppender<Page> writer = partitionToWriter.getValue().getWriter();
            Page partition = page.getPositions(Ints.toArray(rowNums), 0, rowNums.size());
            writer.add(partition);
        }

        return NOT_BLOCKED;
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        Collection<Slice> commitTasks = new ArrayList<>();
        for (String partition : partitionToFile.keySet()) {
            String file = partitionToFile.get(partition);
            final FileAppender<Page> fileAppender = partitionToWriterContext.get(partition).getWriter();
            try {
                fileAppender.close();
            }
            catch (IOException e) {
                abort();
                throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Failed to close" + file);
            }
            final Metrics metrics = fileAppender.metrics();
            commitTasks.add(Slices.wrappedBuffer(jsonCodec.toJsonBytes(new CommitTaskData(file, toJson(metrics), partition))));
        }

        return CompletableFuture.completedFuture(commitTasks);
    }

    @Override
    public void abort()
    {
        partitionToFile.values().stream().forEach(s -> {
            try {
                new Path(s).getFileSystem(configuration).delete(new Path(s), false);
            }
            catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        });
    }

    private FileAppender<Page> addWriter(String partitionPath)
    {
        final Path dataDir = new Path(outputDir);
        final String pathUUId = randomUUID().toString();
        final Path outputPath = (partitionPath != null && !partitionPath.isEmpty()) ? new Path(new Path(dataDir, partitionPath), pathUUId) : new Path(dataDir, pathUUId);
        final String outputFilePath = fileFormat.addExtension(outputPath.toString());
        final OutputFile outputFile = HadoopOutputFile.fromPath(new Path(outputFilePath), configuration);
        switch (fileFormat) {
            case PARQUET:
                try {
                    final FileAppender<Page> writer = Parquet.write(outputFile)
                            .set("spark.sql.parquet.writeLegacyFormat", "false")
                            .set("spark.sql.parquet.binaryAsString", "false")
                            .set("spark.sql.parquet.int96AsTimestamp", "false")
                            .set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
                            .schema(outputSchema)
                            .writeSupport(new PrestoWriteSupport(inputColumns, typeToMessageType.convert(outputSchema, "presto_schema"), outputSchema, typeManager, session))
                            .build();
                    partitionToFile.put(partitionPath, outputFile.location());
                    return writer;
                }
                catch (IOException e) {
                    throw new RuntimeIOException("Could not create writer ", e);
                }
            case ORC:
            case AVRO:
            default:
                throw new UnsupportedOperationException("Only parquet is supported for iceberg as of now");
        }
    }

    private final String getPartitionPath(Page page, int rowNum)
    {
        // TODO only handles identity columns right now, handle all transforms
        List<String> paths = new ArrayList<>();
        for (int i = 0; i < partitionColumns.size(); i++) {
            final HiveColumnHandle columnHandle = partitionColumns.get(i);
            final Type type = partitionTypes.get(i);
            paths.add(columnHandle.getName() + "=" + HiveWriteUtils.getField(type, page.getBlock(columnHandle.getHiveColumnIndex()), rowNum));
        }

        return PATH_JOINER.join(paths);
    }

    private class PartitionWriteContext
    {
        private final List<Integer> rowNum;
        private final FileAppender<Page> writer;

        private PartitionWriteContext(List<Integer> rowNum, FileAppender<Page> writer)
        {
            this.rowNum = rowNum;
            this.writer = writer;
        }

        public List<Integer> getRowNum()
        {
            return rowNum;
        }

        public FileAppender<Page> getWriter()
        {
            return writer;
        }
    }
}
