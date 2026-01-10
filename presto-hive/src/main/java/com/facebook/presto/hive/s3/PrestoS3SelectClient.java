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
package com.facebook.presto.hive.s3;

import com.facebook.presto.hive.HiveClientConfig;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;

import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * Client for executing S3 Select queries.
 * Follows AWS SDK best practices using the Visitor pattern.
 * See: https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/java_s3_code_examples.html
 */
public class PrestoS3SelectClient
        implements Closeable
{
    private final S3AsyncClient s3AsyncClient;
    private final AtomicBoolean requestComplete = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final AtomicReference<CompletableFuture<?>> activeRequest = new AtomicReference<>();

    private volatile String bucketName;
    private volatile String keyName;

    public PrestoS3SelectClient(Configuration config, HiveClientConfig clientConfig, PrestoS3ClientFactory s3ClientFactory)
    {
        requireNonNull(config, "config is null");
        requireNonNull(clientConfig, "clientConfig is null");
        requireNonNull(s3ClientFactory, "s3ClientFactory is null");
        this.s3AsyncClient = s3ClientFactory.getS3AsyncClient(config, clientConfig);
    }

    /**
     * Execute S3 Select query and return the results as an InputStream.
     * Uses the AWS SDK recommended Visitor pattern for handling events.
     */
    public InputStream getRecordsContent(SelectObjectContentRequest selectObjectRequest)
    {
        if (closed.get()) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "S3 Select client is closed");
        }

        requireNonNull(selectObjectRequest, "selectObjectRequest is null");

        this.bucketName = selectObjectRequest.bucket();
        this.keyName = selectObjectRequest.key();
        this.requestComplete.set(false);

        PipedOutputStream outputStream = new PipedOutputStream();
        PipedInputStream inputStream;
        try {
            inputStream = new PipedInputStream(outputStream);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed to create S3 Select stream", e);
        }

        AtomicReference<Throwable> error = new AtomicReference<>();

        SelectObjectContentResponseHandler.Visitor visitor =
                SelectObjectContentResponseHandler.Visitor.builder()
                        .onRecords(recordsEvent -> {
                            try {
                                if (recordsEvent.payload() != null) {
                                    byte[] data = recordsEvent.payload().asByteArray();
                                    if (data.length > 0) {
                                        outputStream.write(data);
                                    }
                                }
                            }
                            catch (IOException e) {
                                // Reader likely closed early. Stop writing.
                                error.compareAndSet(null, e);
                                closeQuietly(outputStream);
                            }
                        })
                        .onEnd(endEvent -> {
                            requestComplete.set(true);
                            closeQuietly(outputStream);
                        })
                        .build();

        SelectObjectContentResponseHandler handler =
                SelectObjectContentResponseHandler.builder()
                        .subscriber(visitor)
                        .onError(throwable -> {
                            error.set(throwable);
                            closeQuietly(outputStream);
                        })
                        .build();

        CompletableFuture<Void> selectFuture = s3AsyncClient.selectObjectContent(selectObjectRequest, handler);
        activeRequest.set(selectFuture);

        return new FilterInputStream(inputStream)
        {
            @Override
            public void close() throws IOException
            {
                try {
                    selectFuture.cancel(true);
                }
                finally {
                    activeRequest.compareAndSet(selectFuture, null);
                    super.close();
                }
            }

            @Override
            public int read(byte[] b, int off, int len) throws IOException
            {
                int bytesRead = super.read(b, off, len);
                if (bytesRead < 0 && error.get() != null) {
                    throw new IOException(
                            "S3 Select operation failed for s3://" + getBucketName() + "/" + getKeyName(),
                            error.get());
                }
                return bytesRead;
            }
        };
    }

    private static void closeQuietly(OutputStream stream)
    {
        try {
            stream.close();
        }
        catch (IOException ignored) {
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        CompletableFuture<?> request = activeRequest.getAndSet(null);
        if (request != null) {
            request.cancel(true);
        }
    }

    public String getKeyName()
    {
        return keyName;
    }

    public String getBucketName()
    {
        return bucketName;
    }

    public boolean isRequestComplete()
    {
        return requestComplete.get();
    }
}
