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
package com.facebook.presto.hive;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.common.io.DataSink;
import com.facebook.presto.hive.datasink.DataSinkFactory;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.mapred.JobConf;

import javax.inject.Inject;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.facebook.airlift.concurrent.MoreFutures.getFutureValue;
import static com.facebook.presto.common.io.DataOutput.createDataOutput;
import static com.facebook.presto.hive.HiveCompressionCodec.NONE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.hive.HiveWriteUtils.initializeSerializer;
import static com.facebook.presto.hive.pagefile.PageFileWriterFactory.createEmptyPageFile;
import static com.facebook.presto.hive.util.ConfigurationUtils.configureCompression;
import static com.google.common.util.concurrent.Futures.whenAllSucceed;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static java.lang.String.format;
import static java.nio.file.Files.deleteIfExists;
import static java.nio.file.Files.readAllBytes;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;

public class HiveZeroRowFileCreator
        implements ZeroRowFileCreator
{
    private static final Logger log = Logger.get(HiveZeroRowFileCreator.class);

    private final HdfsEnvironment hdfsEnvironment;
    private final DataSinkFactory dataSinkFactory;
    private final ListeningExecutorService executor;

    @Inject
    public HiveZeroRowFileCreator(
            HdfsEnvironment hdfsEnvironment,
            DataSinkFactory dataSinkFactory,
            @ForZeroRowFileCreator ListeningExecutorService zeroRowFileCreatorExecutor)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.dataSinkFactory = requireNonNull(dataSinkFactory, "dataSinkFactory is null");
        this.executor = requireNonNull(zeroRowFileCreatorExecutor, "zeroRowFileCreatorExecutor is null");
    }

    @Override
    public void createFiles(ConnectorSession session, HdfsContext hdfsContext, Path destinationDirectory, List<String> fileNames, StorageFormat format, HiveCompressionCodec compressionCodec, Properties schema)
    {
        if (fileNames.isEmpty()) {
            return;
        }
        byte[] fileContent = generateZeroRowFile(session, hdfsContext, schema, format.getSerDe(), format.getOutputFormat(), compressionCodec);

        List<ListenableFuture<?>> commitFutures = new ArrayList<>();

        for (String fileName : fileNames) {
            commitFutures.add(executor.submit(() -> createFile(hdfsContext, new Path(destinationDirectory, fileName), fileContent, session)));
        }

        ListenableFuture<?> listenableFutureAggregate = whenAllSucceed(commitFutures).call(() -> null, directExecutor());
        try {
            getFutureValue(listenableFutureAggregate, PrestoException.class);
        }
        catch (RuntimeException e) {
            listenableFutureAggregate.cancel(true);
            throw e;
        }
    }

    private byte[] generateZeroRowFile(
            ConnectorSession session,
            HdfsContext hdfsContext,
            Properties properties,
            String serDe,
            String outputFormatName,
            HiveCompressionCodec compressionCodec)
    {
        String tmpDirectoryPath = System.getProperty("java.io.tmpdir");
        String tmpFileName = format("presto-hive-zero-row-file-creator-%s-%s", session.getQueryId(), randomUUID().toString());
        java.nio.file.Path tmpFilePath = Paths.get(tmpDirectoryPath, tmpFileName);

        try {
            Path target = new Path(format("file://%s/%s", tmpDirectoryPath, tmpFileName));

            //https://github.com/prestodb/presto/issues/14401 JSON Format reader does not fetch compression from source system
            JobConf conf = configureCompression(
                    hdfsEnvironment.getConfiguration(hdfsContext, target),
                    outputFormatName.equals(HiveStorageFormat.JSON.getOutputFormat()) ? compressionCodec : NONE);

            if (outputFormatName.equals(HiveStorageFormat.PAGEFILE.getOutputFormat())) {
                createEmptyPageFile(dataSinkFactory, session, target.getFileSystem(conf), target);
                return readAllBytes(tmpFilePath);
            }

            // Some serializers such as Avro set a property in the schema.
            initializeSerializer(conf, properties, serDe);

            // The code below is not a try with resources because RecordWriter is not Closeable.
            RecordWriter recordWriter = HiveWriteUtils.createRecordWriter(
                    target,
                    conf,
                    properties,
                    outputFormatName,
                    session);
            recordWriter.close(false);

            return readAllBytes(tmpFilePath);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        finally {
            try {
                deleteIfExists(tmpFilePath);
            }
            catch (IOException e) {
                log.error(e, "Error deleting temporary file: %s", tmpFilePath);
            }
        }
    }

    private void createFile(HdfsContext hdfsContext, Path path, byte[] content, ConnectorSession session)
    {
        try {
            FileSystem fs = hdfsEnvironment.getFileSystem(hdfsContext, path);
            try (DataSink dataSink = dataSinkFactory.createDataSink(session, fs, path)) {
                DataOutput dataOutput = createDataOutput(Slices.wrappedBuffer(content));
                dataSink.write(ImmutableList.of(dataOutput));
            }
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error write zero-row file to Hive", e);
        }
    }
}
