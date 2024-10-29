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

import com.amazonaws.AbortedException;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;

import java.io.ByteArrayInputStream;
import java.io.EOFException;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.presto.hive.RetryDriver.retry;
import static com.facebook.presto.hive.s3.PrestoS3FileSystem.BACKOFF_MIN_SLEEP;
import static com.facebook.presto.hive.s3.PrestoS3FileSystem.keyFromPath;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.fs.FSExceptionMessages.CANNOT_SEEK_PAST_EOF;
import static org.apache.hadoop.fs.FSExceptionMessages.NEGATIVE_SEEK;
import static org.apache.hadoop.fs.FSExceptionMessages.STREAM_IS_CLOSED;

public class PrestoS3InputStream
        extends FSInputStream
{
    private static final DataSize MAX_SKIP_SIZE = new DataSize(1, MEGABYTE);
    private static final int HTTP_RANGE_NOT_SATISFIABLE = 416;

    private final AmazonS3 s3;
    private final String host;
    private final Path path;
    private final int maxAttempts;
    private final Duration maxBackoffTime;
    private final Duration maxRetryTime;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final PrestoS3FileSystemStats stats;

    private InputStream in;
    private long streamPosition;
    private long nextReadPosition;

    public PrestoS3InputStream(AmazonS3 s3, String host, Path path, int maxAttempts, Duration maxBackoffTime, Duration maxRetryTime, PrestoS3FileSystemStats stats)
    {
        this.s3 = requireNonNull(s3, "s3 is null");
        this.host = requireNonNull(host, "host is null");
        this.path = requireNonNull(path, "path is null");

        checkArgument(maxAttempts >= 0, "maxAttempts cannot be negative");
        this.maxAttempts = maxAttempts;
        this.maxBackoffTime = requireNonNull(maxBackoffTime, "maxBackoffTime is null");
        this.maxRetryTime = requireNonNull(maxRetryTime, "maxRetryTime is null");
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public void close()
    {
        closed.set(true);
        closeStream();
    }

    @Override
    public int read(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        checkClosed();
        if (position < 0) {
            throw new EOFException(NEGATIVE_SEEK);
        }
        checkPositionIndexes(offset, offset + length, buffer.length);
        if (length == 0) {
            return 0;
        }

        try {
            return retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(BACKOFF_MIN_SLEEP, maxBackoffTime, maxRetryTime, 2.0)
                    .stopOn(InterruptedException.class, PrestoS3FileSystem.UnrecoverableS3OperationException.class, EOFException.class, FileNotFoundException.class, AbortedException.class)
                    .onRetry(stats::newGetObjectRetry)
                    .run("getS3Object", () -> getS3Object(position, buffer, offset, length));
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private int getS3Object(long position, byte[] buffer, int offset, int length)
            throws IOException
    {
        InputStream stream;
        try {
            GetObjectRequest request = new GetObjectRequest(host, keyFromPath(path))
                    .withRange(position, (position + length) - 1);
            stream = s3.getObject(request).getObjectContent();
        }
        catch (AmazonS3Exception e) {
            stats.newGetObjectError();
            switch (e.getStatusCode()) {
                case HTTP_RANGE_NOT_SATISFIABLE:
                    throw new EOFException(CANNOT_SEEK_PAST_EOF);
                case HTTP_NOT_FOUND:
                    throw new FileNotFoundException("File does not exist: " + path);
                case HTTP_FORBIDDEN:
                case HTTP_BAD_REQUEST:
                    throw new PrestoS3FileSystem.UnrecoverableS3OperationException(path, e);
                default:
                    throw e;
            }
        }
        catch (RuntimeException e) {
            stats.newGetObjectError();
            throw e;
        }

        stats.connectionOpened();
        try {
            int read = 0;
            while (read < length) {
                int n = stream.read(buffer, offset + read, length - read);
                if (n <= 0) {
                    if (read > 0) {
                        return read;
                    }
                    return -1;
                }
                read += n;
            }
            return read;
        }
        catch (Throwable t) {
            stats.newReadError(t);
            abortStream(stream);
            throw t;
        }
        finally {
            stats.connectionReleased();
            stream.close();
        }
    }

    @Override
    public void seek(long pos)
            throws IOException
    {
        checkClosed();
        if (pos < 0) {
            throw new EOFException(NEGATIVE_SEEK);
        }

        // this allows a seek beyond the end of the stream but the next read will fail
        nextReadPosition = pos;
    }

    @Override
    public long getPos()
    {
        return nextReadPosition;
    }

    @Override
    public int read()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read(byte[] buffer, int offset, int length)
            throws IOException
    {
        checkClosed();
        try {
            int bytesRead = retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(BACKOFF_MIN_SLEEP, maxBackoffTime, maxRetryTime, 2.0)
                    .stopOn(InterruptedException.class, PrestoS3FileSystem.UnrecoverableS3OperationException.class, AbortedException.class, FileNotFoundException.class)
                    .onRetry(stats::newReadRetry)
                    .run("readStream", () -> {
                        seekStream();
                        try {
                            return in.read(buffer, offset, length);
                        }
                        catch (Exception e) {
                            stats.newReadError(e);
                            closeStream();
                            throw e;
                        }
                    });

            if (bytesRead != -1) {
                streamPosition += bytesRead;
                nextReadPosition += bytesRead;
            }
            return bytesRead;
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    @Override
    public boolean seekToNewSource(long targetPos)
    {
        return false;
    }

    private void seekStream()
            throws IOException
    {
        if ((in != null) && (nextReadPosition == streamPosition)) {
            // already at specified position
            return;
        }

        if ((in != null) && (nextReadPosition > streamPosition)) {
            // seeking forwards
            long skip = nextReadPosition - streamPosition;
            if (skip <= Math.max(in.available(), MAX_SKIP_SIZE.toBytes())) {
                // already buffered or seek is small enough
                try {
                    if (in.skip(skip) == skip) {
                        streamPosition = nextReadPosition;
                        return;
                    }
                }
                catch (IOException ignored) {
                    // will retry by re-opening the stream
                }
            }
        }

        // close the stream and open at desired position
        streamPosition = nextReadPosition;
        closeStream();
        openStream();
    }

    private void openStream()
            throws IOException
    {
        if (in == null) {
            in = openStream(path, nextReadPosition);
            streamPosition = nextReadPosition;
            stats.connectionOpened();
        }
    }

    private InputStream openStream(Path path, long start)
            throws IOException
    {
        try {
            return retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(BACKOFF_MIN_SLEEP, maxBackoffTime, maxRetryTime, 2.0)
                    .stopOn(InterruptedException.class, PrestoS3FileSystem.UnrecoverableS3OperationException.class, FileNotFoundException.class, AbortedException.class)
                    .onRetry(stats::newGetObjectRetry)
                    .run("getS3Object", () -> getS3Object(path, start));
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private InputStream getS3Object(Path path, long start)
            throws IOException
    {
        try {
            GetObjectRequest request = new GetObjectRequest(host, keyFromPath(path)).withRange(start);
            return s3.getObject(request).getObjectContent();
        }
        catch (AmazonS3Exception e) {
            stats.newGetObjectError();
            switch (e.getStatusCode()) {
                case HTTP_RANGE_NOT_SATISFIABLE:
                    // ignore request for start past end of object
                    return new ByteArrayInputStream(new byte[0]);
                case HTTP_NOT_FOUND:
                    throw new FileNotFoundException("File does not exist: " + path);
                case HTTP_FORBIDDEN:
                case HTTP_BAD_REQUEST:
                    throw new PrestoS3FileSystem.UnrecoverableS3OperationException(path, e);
                default:
                    throw e;
            }
        }
        catch (RuntimeException e) {
            stats.newGetObjectError();
            throw e;
        }
    }

    private void closeStream()
    {
        if (in != null) {
            abortStream(in);
            in = null;
            stats.connectionReleased();
        }
    }

    private void checkClosed()
            throws IOException
    {
        if (closed.get()) {
            throw new IOException(STREAM_IS_CLOSED);
        }
    }

    private static void abortStream(InputStream in)
    {
        try {
            if (in instanceof S3ObjectInputStream) {
                ((S3ObjectInputStream) in).abort();
            }
            else {
                in.close();
            }
        }
        catch (IOException | AbortedException ignored) {
            // thrown if the current thread is in the interrupted state
        }
    }

    private static RuntimeException propagate(Exception e)
            throws IOException
    {
        if (e instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }
        throwIfInstanceOf(e, IOException.class);
        throwIfUnchecked(e);
        throw new IOException(e);
    }
}
