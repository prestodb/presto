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

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
    private SelectObjectContentRequest selectObjectRequest;

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
        this.selectObjectRequest = requireNonNull(selectObjectRequest, "selectObjectRequest is null");
        this.requestComplete.set(false);

        List<byte[]> recordsData = new ArrayList<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean(false);

        try {
            SelectObjectContentResponseHandler.Visitor visitor = SelectObjectContentResponseHandler.Visitor.builder()
                    .onRecords(recordsEvent -> {
                        if (recordsEvent.payload() != null) {
                            byte[] data = recordsEvent.payload().asByteArray();
                            if (data != null && data.length > 0) {
                                recordsData.add(data);
                            }
                        }
                    })
                    .onEnd(endEvent -> {
                        completed.set(true);
                        requestComplete.set(true);
                    })
                    .build();

            SelectObjectContentResponseHandler handler = SelectObjectContentResponseHandler.builder()
                    .subscriber(visitor)
                    .onError(throwable -> {
                        errorRef.set(throwable);
                    })
                    .build();

            CompletableFuture<Void> selectOperation = s3AsyncClient.selectObjectContent(selectObjectRequest, handler);
            selectOperation.join();

            if (errorRef.get() != null) {
                throw new PrestoException(HIVE_FILESYSTEM_ERROR,
                        "S3 Select operation failed for s3://" + getBucketName() + "/" + getKeyName(),
                        errorRef.get());
            }

            if (recordsData.isEmpty()) {
                return new ByteArrayInputStream(new byte[0]);
            }

            return new SequenceInputStream(Collections.enumeration(
                    recordsData.stream()
                            .map(ByteArrayInputStream::new)
                            .collect(java.util.stream.Collectors.toList())));
        }
        catch (Exception e) {
            if (e instanceof PrestoException) {
                throw (PrestoException) e;
            }
            throw new PrestoException(HIVE_FILESYSTEM_ERROR,
                    "S3 Select operation failed for s3://" + getBucketName() + "/" + getKeyName(),
                    e);
        }
    }

    @Override
    public void close()
            throws IOException
    {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
    }

    public String getKeyName()
    {
        return selectObjectRequest != null ? selectObjectRequest.key() : null;
    }

    public String getBucketName()
    {
        return selectObjectRequest != null ? selectObjectRequest.bucket() : null;
    }

    public boolean isRequestComplete()
    {
        return requestComplete.get();
    }
}
