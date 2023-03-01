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
package com.facebook.presto.hive.s3select;

import com.amazonaws.AbortedException;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.CompressionType;
import com.amazonaws.services.s3.model.InputSerialization;
import com.amazonaws.services.s3.model.OutputSerialization;
import com.amazonaws.services.s3.model.ScanRange;
import com.amazonaws.services.s3.model.SelectObjectContentRequest;
import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.hive.s3.HiveS3Config;
import com.facebook.presto.hive.s3.PrestoS3ClientFactory;
import com.facebook.presto.hive.s3.PrestoS3FileSystem;
import com.facebook.presto.hive.s3.PrestoS3SelectClient;
import com.facebook.presto.spi.PrestoException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.Closer;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.BZip2Codec;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.LineReader;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static com.amazonaws.services.s3.model.ExpressionType.SQL;
import static com.facebook.presto.hive.RetryDriver.retry;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_MAX_BACKOFF_TIME;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_MAX_CLIENT_RETRIES;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_MAX_RETRY_TIME;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.hive.serde.serdeConstants.FIELD_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.LINE_DELIM;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;

public abstract class S3SelectLineRecordReader
        implements RecordReader<LongWritable, Text>
{
    private InputStream selectObjectContent;
    private long processedRecords;
    private long recordsFromS3;
    private long position;
    private LineReader reader;
    private boolean isFirstLine;
    private static final Duration BACKOFF_MIN_SLEEP = new Duration(1, SECONDS);
    private final PrestoS3SelectClient selectClient;
    private final long start;
    private final long end;
    private final int maxAttempts;
    private final Duration maxBackoffTime;
    private final Duration maxRetryTime;
    private final Closer closer = Closer.create();
    private final SelectObjectContentRequest selectObjectContentRequest;
    protected final CompressionCodecFactory compressionCodecFactory;
    private final String lineDelimiter;
    private final Properties schema;
    private final CompressionType compressionType;
    private long fileSize;

    S3SelectLineRecordReader(
            Configuration configuration,
            HiveClientConfig clientConfig,
            Path path,
            long start,
            long length,
            long fileSize,
            Properties schema,
            String ionSqlQuery,
            PrestoS3ClientFactory s3ClientFactory)
    {
        this.fileSize = fileSize;
        requireNonNull(configuration, "configuration is null");
        requireNonNull(clientConfig, "clientConfig is null");
        requireNonNull(schema, "schema is null");
        requireNonNull(path, "path is null");
        requireNonNull(ionSqlQuery, "ionSqlQuery is null");
        requireNonNull(s3ClientFactory, "s3ClientFactory is null");
        this.lineDelimiter = (schema).getProperty(LINE_DELIM, "\n");
        this.processedRecords = 0;
        this.recordsFromS3 = 0;
        this.start = start;
        this.position = this.start;
        this.end = this.start + length;
        this.fileSize = fileSize;
        this.isFirstLine = true;

        this.compressionCodecFactory = new CompressionCodecFactory(configuration);
        this.compressionType = getCompressionType(path);
        this.schema = schema;
        this.selectObjectContentRequest = buildSelectObjectRequest(ionSqlQuery, path);

        HiveS3Config defaults = new HiveS3Config();
        this.maxAttempts = configuration.getInt(S3_MAX_CLIENT_RETRIES, defaults.getS3MaxClientRetries()) + 1;
        this.maxBackoffTime = Duration.valueOf(configuration.get(S3_MAX_BACKOFF_TIME, defaults.getS3MaxBackoffTime().toString()));
        this.maxRetryTime = Duration.valueOf(configuration.get(S3_MAX_RETRY_TIME, defaults.getS3MaxRetryTime().toString()));

        this.selectClient = new PrestoS3SelectClient(configuration, clientConfig, s3ClientFactory);
        closer.register(selectClient);
    }

    protected abstract InputSerialization buildInputSerialization();

    protected abstract OutputSerialization buildOutputSerialization();

    protected Properties getSchema()
    {
        return schema;
    }

    protected CompressionType getCompressionType()
    {
        return compressionType;
    }

    protected String getLineDelimiter()
    {
        return lineDelimiter;
    }

    protected long getStart()
    {
        return start;
    }

    protected long getEnd()
    {
        return end;
    }

    public SelectObjectContentRequest buildSelectObjectRequest(String query, Path path)
    {
        SelectObjectContentRequest selectObjectRequest = new SelectObjectContentRequest();
        URI uri = path.toUri();
        selectObjectRequest.setBucketName(PrestoS3FileSystem.getBucketName(uri));
        selectObjectRequest.setKey(PrestoS3FileSystem.keyFromPath(path));
        selectObjectRequest.setExpression(query);
        selectObjectRequest.setExpressionType(SQL);

        InputSerialization selectObjectInputSerialization = buildInputSerialization();
        selectObjectRequest.setInputSerialization(selectObjectInputSerialization);

        OutputSerialization selectObjectOutputSerialization = buildOutputSerialization();
        selectObjectRequest.setOutputSerialization(selectObjectOutputSerialization);

        if (start != 0 || end != fileSize) {
            ScanRange scanRange = new ScanRange();
            scanRange.setStart(start);
            scanRange.setEnd(end);
            selectObjectRequest.setScanRange(scanRange);
        }

        return selectObjectRequest;
    }

    protected CompressionType getCompressionType(Path path)
    {
        CompressionCodec codec = compressionCodecFactory.getCodec(path);
        if (codec == null) {
            return CompressionType.NONE;
        }
        if (codec instanceof GzipCodec) {
            return CompressionType.GZIP;
        }
        if (codec instanceof BZip2Codec) {
            return CompressionType.BZIP2;
        }
        throw new PrestoException(NOT_SUPPORTED, "Compression extension not supported for S3 Select: " + path);
    }

    private int readLine(Text value)
            throws IOException
    {
        try {
            return retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(BACKOFF_MIN_SLEEP, maxBackoffTime, maxRetryTime, 2.0)
                    .stopOn(InterruptedException.class, UnrecoverableS3OperationException.class, AbortedException.class)
                    .run("readRecordsContentStream", () -> {
                        if (isFirstLine) {
                            recordsFromS3 = 0;
                            selectObjectContent = selectClient.getRecordsContent(selectObjectContentRequest);
                            closer.register(selectObjectContent);
                            reader = new LineReader(selectObjectContent, lineDelimiter.getBytes(StandardCharsets.UTF_8));
                            closer.register(reader);
                            isFirstLine = false;
                        }
                        try {
                            return reader.readLine(value);
                        }
                        catch (RuntimeException e) {
                            isFirstLine = true;
                            recordsFromS3 = 0;
                            if (e instanceof AmazonS3Exception) {
                                switch (((AmazonS3Exception) e).getStatusCode()) {
                                    case HTTP_FORBIDDEN:
                                    case HTTP_NOT_FOUND:
                                    case HTTP_BAD_REQUEST:
                                        throw new UnrecoverableS3OperationException(selectClient.getBucketName(), selectClient.getKeyName(), e);
                                }
                            }
                            throw e;
                        }
                    });
        }
        catch (Exception e) {
            throwIfInstanceOf(e, IOException.class);
            throwIfUnchecked(e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public synchronized boolean next(LongWritable key, Text value)
            throws IOException
    {
        while (true) {
            int bytes = readLine(value);
            if (bytes <= 0) {
                if (!selectClient.isRequestComplete()) {
                    throw new IOException("S3 Select request was incomplete as End Event was not received");
                }
                return false;
            }
            recordsFromS3++;
            if (recordsFromS3 > processedRecords) {
                position += bytes;
                processedRecords++;
                key.set(processedRecords);
                return true;
            }
        }
    }

    @Override
    public LongWritable createKey()
    {
        return new LongWritable();
    }

    @Override
    public Text createValue()
    {
        return new Text();
    }

    @Override
    public long getPos()
    {
        return position;
    }

    @Override
    public void close()
            throws IOException
    {
        closer.close();
    }

    @Override
    public float getProgress()
    {
        return ((float) (position - start)) / (end - start);
    }

    String getFieldDelimiter(Properties schema)
    {
        return schema.getProperty(FIELD_DELIM, schema.getProperty(SERIALIZATION_FORMAT));
    }

    /**
     * This exception is for stopping retries for S3 Select calls that shouldn't be retried.
     * For example, "Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: Forbidden (Service: Amazon S3; Status Code: 403 ..."
     */
    @VisibleForTesting
    public static class UnrecoverableS3OperationException
            extends RuntimeException
    {
        public UnrecoverableS3OperationException(String bucket, String key, Throwable cause)
        {
            // append bucket and key to the message
            super(format("%s (Bucket: %s, Key: %s)", cause, bucket, key));
        }
    }
}
