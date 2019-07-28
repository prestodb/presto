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

import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.mapred.JobConf;

import javax.inject.Inject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.hive.HiveWriteUtils.initializeSerializer;
import static com.facebook.presto.hive.util.ConfigurationUtils.configureCompression;
import static com.google.common.util.concurrent.Futures.whenAllSucceed;
import static com.google.common.util.concurrent.MoreExecutors.directExecutor;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;

public class HiveZeroRowFileCreator
        implements ZeroRowFileCreator
{
    private final HdfsEnvironment hdfsEnvironment;
    private final ListeningExecutorService executor;

    @Inject
    public HiveZeroRowFileCreator(
            HdfsEnvironment hdfsEnvironment,
            @ForZeroRowFileCreator ListeningExecutorService zeroRowFileCreatorExecutor)
    {
        this.hdfsEnvironment = requireNonNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.executor = requireNonNull(zeroRowFileCreatorExecutor, "zeroRowFileCreatorExecutor is null");
    }

    @Override
    public void createFiles(ConnectorSession session, HdfsContext hdfsContext, Path destinationDirectory, List<String> fileNames, StorageFormat format, HiveCompressionCodec compressionCodec, Properties schema)
    {
        JobConf conf = configureCompression(hdfsEnvironment.getConfiguration(hdfsContext, destinationDirectory), compressionCodec);
        List<ListenableFuture<?>> commitFutures = new ArrayList<>();

        for (String fileName : fileNames) {
            commitFutures.add(
                    executor.submit(
                            () -> writeZeroRowFile(session, new Path(destinationDirectory, fileName), conf, schema, format.getSerDe(), format.getOutputFormat())));
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

    private static void writeZeroRowFile(ConnectorSession session, Path target, JobConf conf, Properties properties, String serDe, String outputFormatName)
    {
        // Some serializers such as Avro set a property in the schema.
        initializeSerializer(conf, properties, serDe);

        // The code below is not a try with resources because RecordWriter is not Closeable.
        RecordWriter recordWriter = HiveWriteUtils.createRecordWriter(target, conf, properties, outputFormatName, session);
        try {
            recordWriter.close(false);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error write zero-row file to Hive", e);
        }
    }
}
