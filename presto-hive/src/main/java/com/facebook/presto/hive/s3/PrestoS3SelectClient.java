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
import software.amazon.awssdk.services.s3.model.EndEvent;
import software.amazon.awssdk.services.s3.model.RecordsEvent;
import software.amazon.awssdk.services.s3.model.SelectObjectContentEventStream;
import software.amazon.awssdk.services.s3.model.SelectObjectContentRequest;
import software.amazon.awssdk.services.s3.model.SelectObjectContentResponseHandler;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Objects.requireNonNull;

public class PrestoS3SelectClient
        implements Closeable
{
    private static final int PIPE_BUFFER_SIZE = 1024 * 1024;

    private final S3AsyncClient s3AsyncClient;
    private final boolean ownClient;
    private final AtomicBoolean requestComplete = new AtomicBoolean(false);
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private SelectObjectContentRequest selectObjectRequest;
    private CompletableFuture<Void> selectOperation;
    private volatile Throwable streamError;

    public PrestoS3SelectClient(Configuration config, HiveClientConfig clientConfig, PrestoS3ClientFactory s3ClientFactory)
    {
        requireNonNull(config, "config is null");
        requireNonNull(clientConfig, "clientConfig is null");
        requireNonNull(s3ClientFactory, "s3ClientFactory is null");

        this.s3AsyncClient = s3ClientFactory.getS3AsyncClient(config, clientConfig);
        this.ownClient = false;
    }

    public InputStream getRecordsContent(SelectObjectContentRequest selectObjectRequest)
    {
        this.selectObjectRequest = requireNonNull(selectObjectRequest, "selectObjectRequest is null");
        this.requestComplete.set(false);
        this.streamError = null;

        try {
            PipedInputStream inputStream = new PipedInputStream(PIPE_BUFFER_SIZE);
            PipedOutputStream outputStream = new PipedOutputStream(inputStream);

            CountDownLatch subscriptionLatch = new CountDownLatch(1);
            AtomicBoolean subscriptionStarted = new AtomicBoolean(false);

            SelectObjectContentResponseHandler handler = SelectObjectContentResponseHandler.builder()
                    .onEventStream(publisher -> {
                        if (subscriptionStarted.compareAndSet(false, true)) {
                            subscriptionLatch.countDown();
                            handleEventStream(publisher, outputStream);
                        }
                    })
                    .onError(throwable -> {
                        streamError = throwable;
                        closeStreamAndMarkComplete(outputStream);
                        subscriptionLatch.countDown();
                    })
                    .build();

            this.selectOperation = s3AsyncClient.selectObjectContent(selectObjectRequest, handler);

            this.selectOperation.whenComplete((result, error) -> {
                if (error != null) {
                    streamError = error;
                }
            });

            try {
                if (!subscriptionLatch.await(30, TimeUnit.SECONDS)) {
                    closeStreamAndMarkComplete(outputStream);
                    throw new RuntimeException("Timeout waiting for S3 Select subscription");
                }
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                closeStreamAndMarkComplete(outputStream);
                throw new RuntimeException("Interrupted while waiting for S3 Select subscription", e);
            }

            if (streamError != null) {
                throw new RuntimeException("S3 Select operation failed", streamError);
            }

            return new ErrorTrackingInputStream(inputStream, this);
        }
        catch (Exception e) {
            throw new RuntimeException("S3 Select operation failed", e);
        }
    }

    private void handleEventStream(SdkPublisher<SelectObjectContentEventStream> publisher, PipedOutputStream outputStream)
    {
        publisher.subscribe(new org.reactivestreams.Subscriber<SelectObjectContentEventStream>() {
            private org.reactivestreams.Subscription subscription;
            private volatile boolean completed;

            @Override
            public void onSubscribe(org.reactivestreams.Subscription s)
            {
                this.subscription = s;
                s.request(Long.MAX_VALUE);
            }

            @Override
            public void onNext(SelectObjectContentEventStream event)
            {
                if (completed) {
                    return;
                }

                try {
                    if (event instanceof RecordsEvent) {
                        RecordsEvent recordsEvent = (RecordsEvent) event;
                        if (recordsEvent.payload() != null) {
                            byte[] data = recordsEvent.payload().asByteArray();
                            if (data != null && data.length > 0) {
                                outputStream.write(data);
                                outputStream.flush();
                            }
                        }
                    }
                    else if (event instanceof EndEvent) {
                        completed = true;
                        closeStreamAndMarkComplete(outputStream);
                    }
                }
                catch (IOException e) {
                    streamError = e;
                    completed = true;
                    onError(e);
                }
            }

            @Override
            public void onError(Throwable t)
            {
                if (!completed) {
                    completed = true;
                    streamError = t;
                    closeStreamAndMarkComplete(outputStream);
                }
            }

            @Override
            public void onComplete()
            {
                if (!completed) {
                    completed = true;
                    closeStreamAndMarkComplete(outputStream);
                }
            }
        });
    }

    private void closeStreamAndMarkComplete(PipedOutputStream outputStream)
    {
        try {
            outputStream.close();
        }
        catch (IOException e) {
        }
        finally {
            requestComplete.set(true);
        }
    }

    @Override
    public void close() throws IOException
    {
        if (!closed.compareAndSet(false, true)) {
            return;
        }

        try {
            if (selectOperation != null && !selectOperation.isDone()) {
                try {
                    selectOperation.get(5, TimeUnit.SECONDS);
                }
                catch (Exception e) {
                    selectOperation.cancel(true);
                }
            }
        }
        finally {
            if (ownClient && s3AsyncClient != null) {
                try {
                    s3AsyncClient.close();
                }
                catch (Exception e) {
                }
            }
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

    public void markRequestComplete()
    {
        requestComplete.set(true);
    }

    public Throwable getStreamError()
    {
        return streamError;
    }

    /**
     * Wrapper InputStream that tracks completion and propagates errors
     */
    private static class ErrorTrackingInputStream
            extends InputStream
    {
        private final InputStream delegate;
        private final PrestoS3SelectClient client;
        private boolean closed;

        ErrorTrackingInputStream(InputStream delegate, PrestoS3SelectClient client)
        {
            this.delegate = requireNonNull(delegate, "delegate is null");
            this.client = requireNonNull(client, "client is null");
        }

        @Override
        public int read() throws IOException
        {
            checkError();
            if (closed) {
                return -1;
            }
            int result = delegate.read();
            if (result == -1) {
                client.markRequestComplete();
            }
            return result;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException
        {
            checkError();
            if (closed) {
                return -1;
            }
            int result = delegate.read(b, off, len);
            if (result == -1) {
                client.markRequestComplete();
            }
            return result;
        }

        @Override
        public int available() throws IOException
        {
            checkError();
            if (closed) {
                return 0;
            }
            return delegate.available();
        }

        @Override
        public void close() throws IOException
        {
            if (!closed) {
                closed = true;
                client.markRequestComplete();
                delegate.close();
            }
        }

        private void checkError() throws IOException
        {
            Throwable error = client.getStreamError();
            if (error != null) {
                if (error instanceof IOException) {
                    throw (IOException) error;
                }
                throw new IOException("S3 Select stream error", error);
            }
        }
    }
}
