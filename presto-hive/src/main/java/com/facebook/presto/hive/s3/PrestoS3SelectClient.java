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
import org.apache.hadoop.conf.Configuration;
import software.amazon.awssdk.core.async.SdkPublisher;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.model.ContinuationEvent;
import software.amazon.awssdk.services.s3.model.EndEvent;
import software.amazon.awssdk.services.s3.model.ProgressEvent;
import software.amazon.awssdk.services.s3.model.RecordsEvent;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;
import software.amazon.awssdk.services.s3.model.StatsEvent;

import java.io.Closeable;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class PrestoS3SelectClient
        implements Closeable
{
    private final S3AsyncClient s3AsyncClient;
    private final AtomicBoolean requestComplete = new AtomicBoolean(false);
    private SelectObjectContentRequest selectObjectRequest;
    private CompletableFuture<Void> selectOperation;

    public PrestoS3SelectClient(Configuration config, HiveClientConfig clientConfig, PrestoS3ClientFactory s3ClientFactory)
    {
        requireNonNull(config, "config is null");
        requireNonNull(clientConfig, "clientConfig is null");
        requireNonNull(s3ClientFactory, "s3ClientFactory is null");

        // Get the sync client from factory and create async client with same config
        this.s3AsyncClient = s3ClientFactory.getS3AsyncClient(config, clientConfig);
    }

    public InputStream getRecordsContent(SelectObjectContentRequest selectObjectRequest)
    {
        this.selectObjectRequest = requireNonNull(selectObjectRequest, "selectObjectRequest is null");
        this.requestComplete.set(false);

        try {
            // Create piped streams to convert async response to blocking InputStream
            PipedInputStream inputStream = new PipedInputStream();
            PipedOutputStream outputStream = new PipedOutputStream(inputStream);

            // Create response handler for the streaming response
            SelectObjectContentResponseHandler handler = SelectObjectContentResponseHandler.builder()
                    .onResponse(response -> {
                        // Handle initial response metadata if needed
                    })
                    .onEventStream(publisher -> {
                        // Handle the event stream
                        handleEventStream(publisher, outputStream);
                    })
                    .onError(throwable -> {
                        try {
                            outputStream.close();
                        }
                        catch (IOException e) {
                            // Log error if needed
                        }
                        requestComplete.set(true);
                    })
                    .build();

            // Start the async operation
            this.selectOperation = s3AsyncClient.selectObjectContent(selectObjectRequest, handler);

            return new CompletionTrackingInputStream(inputStream, requestComplete);
        }
        catch (Exception e) {
            throw new RuntimeException("S3 Select operation failed", e);
        }
    }

    private void handleEventStream(SdkPublisher<SelectObjectContentEventStream> publisher, PipedOutputStream outputStream)
    {
        publisher.subscribe(event -> {
            try {
                // Handle different event types using instanceof checks
                if (event instanceof RecordsEvent) {
                    RecordsEvent recordsEvent = (RecordsEvent) event;
                    try {
                        if (recordsEvent.payload() != null) {
                            outputStream.write(recordsEvent.payload().asByteArray());
                        }
                    }
                    catch (IOException e) {
                        closeStreamAndMarkComplete(outputStream);
                    }
                }
                else if (event instanceof StatsEvent) {
                    StatsEvent statsEvent = (StatsEvent) event;
                    // Handle stats event if needed
                    // You can access statsEvent.details() for statistics
                }
                else if (event instanceof ProgressEvent) {
                    ProgressEvent progressEvent = (ProgressEvent) event;
                    // Handle progress event if needed
                    // You can access progressEvent.details() for progress info
                }
                else if (event instanceof EndEvent) {
                    // End event - mark as complete and close stream
                    closeStreamAndMarkComplete(outputStream);
                }
                else if (event instanceof ContinuationEvent) {
                    // Handle continuation event if needed
                    // This is typically used for keep-alive purposes
                }
            }
            catch (Exception e) {
                closeStreamAndMarkComplete(outputStream);
            }
        });
    }

    private void closeStreamAndMarkComplete(PipedOutputStream outputStream)
    {
        try {
            outputStream.close();
        }
        catch (IOException e) {
            // Log error if needed
        }
        requestComplete.set(true);
    }

    @Override
    public void close() throws IOException
    {
        if (selectOperation != null && !selectOperation.isDone()) {
            selectOperation.cancel(true);
        }
        if (s3AsyncClient != null) {
            s3AsyncClient.close();
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

    /**
     * The completion status indicates all matching records have been transmitted.
     * This is set when the End event is received from the event stream.
     */
    public boolean isRequestComplete()
    {
        return requestComplete.get();
    }

    /**
     * Mark the request as complete. This is called automatically when the End event
     * is received, but can be called manually if needed.
     */
    public void markRequestComplete()
    {
        requestComplete.set(true);
    }

    /**
     * Wrapper InputStream that tracks completion
     */
    private static class CompletionTrackingInputStream
            extends FilterInputStream
    {
        private final AtomicBoolean completionFlag;
        private boolean streamClosed;

        protected CompletionTrackingInputStream(InputStream in, AtomicBoolean completionFlag)
        {
            super(in);
            this.completionFlag = completionFlag;
        }

        @Override
        public int read() throws IOException
        {
            int result = super.read();
            if (result == -1 && !streamClosed) {
                markComplete();
            }
            return result;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException
        {
            int result = super.read(b, off, len);
            if (result == -1 && !streamClosed) {
                markComplete();
            }
            return result;
        }

        @Override
        public void close() throws IOException
        {
            if (!streamClosed) {
                markComplete();
            }
            super.close();
        }

        private void markComplete()
        {
            streamClosed = true;
            completionFlag.set(true);
        }
    }
}
