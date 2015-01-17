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

import com.amazonaws.AbortedException;
import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.Protocol;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerConfiguration;
import com.amazonaws.services.s3.transfer.Upload;
import com.google.common.base.Throwables;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.Iterators;
import com.google.common.primitives.Ints;
import io.airlift.log.Logger;
import io.airlift.units.DataSize;
import io.airlift.units.Duration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.s3.S3Credentials;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.Iterables.toArray;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.lang.Math.max;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createTempFile;
import static org.apache.http.HttpStatus.SC_FORBIDDEN;
import static org.apache.http.HttpStatus.SC_NOT_FOUND;

public class PrestoS3FileSystem
        extends FileSystem
{
    public static final String S3_SSL_ENABLED = "presto.s3.ssl.enabled";
    public static final String S3_MAX_ERROR_RETRIES = "presto.s3.max-error-retries";
    public static final String S3_MAX_CLIENT_RETRIES = "presto.s3.max-client-retries";
    public static final String S3_MAX_BACKOFF_TIME = "presto.s3.max-backoff-time";
    public static final String S3_MAX_RETRY_TIME = "presto.s3.max-retry-time";
    public static final String S3_CONNECT_TIMEOUT = "presto.s3.connect-timeout";
    public static final String S3_SOCKET_TIMEOUT = "presto.s3.socket-timeout";
    public static final String S3_MAX_CONNECTIONS = "presto.s3.max-connections";
    public static final String S3_STAGING_DIRECTORY = "presto.s3.staging-directory";
    public static final String S3_MULTIPART_MIN_FILE_SIZE = "presto.s3.multipart.min-file-size";
    public static final String S3_MULTIPART_MIN_PART_SIZE = "presto.s3.multipart.min-part-size";

    private static final Logger log = Logger.get(PrestoS3FileSystem.class);

    private static final DataSize BLOCK_SIZE = new DataSize(32, MEGABYTE);
    private static final DataSize MAX_SKIP_SIZE = new DataSize(1, MEGABYTE);

    private final TransferManagerConfiguration transferConfig = new TransferManagerConfiguration();
    private URI uri;
    private Path workingDirectory;
    private AmazonS3 s3;
    private File stagingDirectory;
    private int maxClientRetries;
    private Duration maxBackoffTime;
    private Duration maxRetryTime;

    @Override
    public void initialize(URI uri, Configuration conf)
            throws IOException
    {
        checkNotNull(uri, "uri is null");
        checkNotNull(conf, "conf is null");
        super.initialize(uri, conf);
        setConf(conf);

        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDirectory = new Path("/").makeQualified(this.uri, new Path("/"));

        HiveClientConfig defaults = new HiveClientConfig();
        this.stagingDirectory = new File(conf.get(S3_STAGING_DIRECTORY, defaults.getS3StagingDirectory().toString()));
        this.maxClientRetries = conf.getInt(S3_MAX_CLIENT_RETRIES, defaults.getS3MaxClientRetries());
        this.maxBackoffTime = Duration.valueOf(conf.get(S3_MAX_BACKOFF_TIME, defaults.getS3MaxBackoffTime().toString()));
        this.maxRetryTime = Duration.valueOf(conf.get(S3_MAX_RETRY_TIME, defaults.getS3MaxRetryTime().toString()));
        int maxErrorRetries = conf.getInt(S3_MAX_ERROR_RETRIES, defaults.getS3MaxErrorRetries());
        boolean sslEnabled = conf.getBoolean(S3_SSL_ENABLED, defaults.isS3SslEnabled());
        Duration connectTimeout = Duration.valueOf(conf.get(S3_CONNECT_TIMEOUT, defaults.getS3ConnectTimeout().toString()));
        Duration socketTimeout = Duration.valueOf(conf.get(S3_SOCKET_TIMEOUT, defaults.getS3SocketTimeout().toString()));
        int maxConnections = conf.getInt(S3_MAX_CONNECTIONS, defaults.getS3MaxConnections());
        long minFileSize = conf.getLong(S3_MULTIPART_MIN_FILE_SIZE, defaults.getS3MultipartMinFileSize().toBytes());
        long minPartSize = conf.getLong(S3_MULTIPART_MIN_PART_SIZE, defaults.getS3MultipartMinPartSize().toBytes());

        ClientConfiguration configuration = new ClientConfiguration();
        configuration.setMaxErrorRetry(maxErrorRetries);
        configuration.setProtocol(sslEnabled ? Protocol.HTTPS : Protocol.HTTP);
        configuration.setConnectionTimeout(Ints.checkedCast(connectTimeout.toMillis()));
        configuration.setSocketTimeout(Ints.checkedCast(socketTimeout.toMillis()));
        configuration.setMaxConnections(maxConnections);

        this.s3 = new AmazonS3Client(getAwsCredentials(uri, conf), configuration);

        transferConfig.setMultipartUploadThreshold(minFileSize);
        transferConfig.setMinimumUploadPartSize(minPartSize);
    }

    @Override
    public URI getUri()
    {
        return uri;
    }

    @Override
    public Path getWorkingDirectory()
    {
        return workingDirectory;
    }

    @Override
    public void setWorkingDirectory(Path path)
    {
        workingDirectory = path;
    }

    @Override
    public FileStatus[] listStatus(Path path)
            throws IOException
    {
        List<LocatedFileStatus> list = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = listLocatedStatus(path);
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return toArray(list, LocatedFileStatus.class);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path path)
            throws IOException
    {
        return new RemoteIterator<LocatedFileStatus>()
        {
            private final Iterator<LocatedFileStatus> iterator = listPrefix(path);

            @Override
            public boolean hasNext()
                    throws IOException
            {
                try {
                    return iterator.hasNext();
                }
                catch (AmazonClientException e) {
                    throw new IOException(e);
                }
            }

            @Override
            public LocatedFileStatus next()
                    throws IOException
            {
                try {
                    return iterator.next();
                }
                catch (AmazonClientException e) {
                    throw new IOException(e);
                }
            }
        };
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        if (path.getName().isEmpty()) {
            // the bucket root requires special handling
            if (getS3ObjectMetadata(path) != null) {
                return new FileStatus(0, true, 1, 0, 0, qualifiedPath(path));
            }
            throw new FileNotFoundException("File does not exist: " + path);
        }

        ObjectMetadata metadata = getS3ObjectMetadata(path);

        if (metadata == null) {
            // check if this path is a directory
            Iterator<LocatedFileStatus> iterator = listPrefix(path);
            if ((iterator != null) && iterator.hasNext()) {
                return new FileStatus(0, true, 1, 0, 0, qualifiedPath(path));
            }
            throw new FileNotFoundException("File does not exist: " + path);
        }

        return new FileStatus(
                metadata.getContentLength(),
                false,
                1,
                BLOCK_SIZE.toBytes(),
                lastModifiedTime(metadata),
                qualifiedPath(path));
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize)
            throws IOException
    {
        return new FSDataInputStream(
                new BufferedFSInputStream(
                        new PrestoS3InputStream(s3, uri.getHost(), path, maxClientRetries, maxBackoffTime, maxRetryTime),
                        bufferSize));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        if ((!overwrite) && exists(path)) {
            throw new IOException("File already exists:" + path);
        }

        createDirectories(stagingDirectory.toPath());
        File tempFile = createTempFile(stagingDirectory.toPath(), "presto-s3-", ".tmp").toFile();

        String key = keyFromPath(qualifiedPath(path));
        return new FSDataOutputStream(
                new PrestoS3OutputStream(s3, transferConfig, uri.getHost(), key, tempFile),
                statistics);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
            throws IOException
    {
        throw new UnsupportedOperationException("append");
    }

    @Override
    public boolean rename(Path src, Path dst)
            throws IOException
    {
        throw new UnsupportedOperationException("rename");
    }

    @Override
    public boolean delete(Path f, boolean recursive)
            throws IOException
    {
        throw new UnsupportedOperationException("delete");
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission)
            throws IOException
    {
        // no need to do anything for S3
        return true;
    }

    private Iterator<LocatedFileStatus> listPrefix(Path path)
    {
        String key = keyFromPath(path);
        if (!key.isEmpty()) {
            key += "/";
        }

        ListObjectsRequest request = new ListObjectsRequest()
                .withBucketName(uri.getHost())
                .withPrefix(key)
                .withDelimiter("/");

        Iterator<ObjectListing> listings = new AbstractSequentialIterator<ObjectListing>(s3.listObjects(request))
        {
            @Override
            protected ObjectListing computeNext(ObjectListing previous)
            {
                if (!previous.isTruncated()) {
                    return null;
                }
                return s3.listNextBatchOfObjects(previous);
            }
        };

        return Iterators.concat(Iterators.transform(listings, this::statusFromListing));
    }

    private Iterator<LocatedFileStatus> statusFromListing(ObjectListing listing)
    {
        return Iterators.concat(
                statusFromPrefixes(listing.getCommonPrefixes()),
                statusFromObjects(listing.getObjectSummaries()));
    }

    private Iterator<LocatedFileStatus> statusFromPrefixes(List<String> prefixes)
    {
        List<LocatedFileStatus> list = new ArrayList<>();
        for (String prefix : prefixes) {
            Path path = qualifiedPath(new Path("/" + prefix));
            FileStatus status = new FileStatus(0, true, 1, 0, 0, path);
            list.add(createLocatedFileStatus(status));
        }
        return list.iterator();
    }

    private Iterator<LocatedFileStatus> statusFromObjects(List<S3ObjectSummary> objects)
    {
        List<LocatedFileStatus> list = new ArrayList<>();
        for (S3ObjectSummary object : objects) {
            if (!object.getKey().endsWith("/")) {
                FileStatus status = new FileStatus(
                        object.getSize(),
                        false,
                        1,
                        BLOCK_SIZE.toBytes(),
                        object.getLastModified().getTime(),
                        qualifiedPath(new Path("/" + object.getKey())));
                list.add(createLocatedFileStatus(status));
            }
        }
        return list.iterator();
    }

    /**
     * This exception is for stopping retries for S3 calls that shouldn't be retried.
     * For example, "Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: Forbidden (Service: Amazon S3; Status Code: 403 ..."
     */
    private static class UnrecoverableS3OperationException extends Exception
    {
        public UnrecoverableS3OperationException(Throwable cause)
        {
            super(cause);
        }
    }

    private ObjectMetadata getS3ObjectMetadata(final Path path)
            throws IOException
    {
        try {
            return retry()
                    .maxAttempts(maxClientRetries)
                    .exponentialBackoff(new Duration(1, TimeUnit.SECONDS), maxBackoffTime, maxRetryTime, 2.0)
                    .stopOn(InterruptedException.class, UnrecoverableS3OperationException.class)
                    .run("getS3ObjectMetadata", () -> {
                        try {
                            return s3.getObjectMetadata(uri.getHost(), keyFromPath(path));
                        }
                        catch (AmazonS3Exception e) {
                            if (e.getStatusCode() == SC_NOT_FOUND) {
                                return null;
                            }
                            else if (e.getStatusCode() == SC_FORBIDDEN) {
                                throw new UnrecoverableS3OperationException(e);
                            }
                            throw Throwables.propagate(e);
                        }
                    });
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
        catch (Exception e) {
            Throwables.propagateIfInstanceOf(e, IOException.class);
            throw Throwables.propagate(e);
        }
    }

    private Path qualifiedPath(Path path)
    {
        return path.makeQualified(this.uri, getWorkingDirectory());
    }

    private LocatedFileStatus createLocatedFileStatus(FileStatus status)
    {
        try {
            BlockLocation[] fakeLocation = getFileBlockLocations(status, 0, status.getLen());
            return new LocatedFileStatus(status, fakeLocation);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static long lastModifiedTime(ObjectMetadata metadata)
    {
        Date date = metadata.getLastModified();
        return (date != null) ? date.getTime() : 0;
    }

    private static String keyFromPath(Path path)
    {
        checkArgument(path.isAbsolute(), "Path is not absolute: %s", path);
        String key = nullToEmpty(path.toUri().getPath());
        if (key.startsWith("/")) {
            key = key.substring(1);
        }
        if (key.endsWith("/")) {
            key = key.substring(0, key.length() - 1);
        }
        return key;
    }

    private static AWSCredentials getAwsCredentials(URI uri, Configuration conf)
    {
        S3Credentials credentials = new S3Credentials();
        credentials.initialize(uri, conf);
        return new BasicAWSCredentials(credentials.getAccessKey(), credentials.getSecretAccessKey());
    }

    private static class PrestoS3InputStream
            extends FSInputStream
    {
        private final AmazonS3 s3;
        private final String host;
        private final Path path;
        private final int maxClientRetry;
        private final Duration maxBackoffTime;
        private final Duration maxRetryTime;

        private boolean closed;
        private S3ObjectInputStream in;
        private long position;

        public PrestoS3InputStream(AmazonS3 s3, String host, Path path, int maxClientRetry, Duration maxBackoffTime, Duration maxRetryTime)
        {
            this.s3 = checkNotNull(s3, "s3 is null");
            this.host = checkNotNull(host, "host is null");
            this.path = checkNotNull(path, "path is null");

            checkArgument(maxClientRetry >= 0, "maxClientRetries cannot be negative");
            this.maxClientRetry = maxClientRetry;
            this.maxBackoffTime = checkNotNull(maxBackoffTime, "maxBackoffTime is null");
            this.maxRetryTime = checkNotNull(maxRetryTime, "maxRetryTime is null");
        }

        @Override
        public void close()
                throws IOException
        {
            closed = true;
            closeStream();
        }

        @Override
        public void seek(long pos)
                throws IOException
        {
            checkState(!closed, "already closed");
            checkArgument(pos >= 0, "position is negative: %s", pos);

            if ((in != null) && (pos == position)) {
                // already at specified position
                return;
            }

            if ((in != null) && (pos > position)) {
                // seeking forwards
                long skip = pos - position;
                if (skip <= max(in.available(), MAX_SKIP_SIZE.toBytes())) {
                    // already buffered or seek is small enough
                    if (in.skip(skip) == skip) {
                        position = pos;
                        return;
                    }
                }
            }

            // close the stream and open at desired position
            position = pos;
            closeStream();
            openStream();
        }

        @Override
        public long getPos()
                throws IOException
        {
            return position;
        }

        @Override
        public int read()
                throws IOException
        {
            // This stream is wrapped with BufferedInputStream, so this method should never be called
            throw new UnsupportedOperationException();
        }

        @Override
        public int read(final byte[] buffer, final int offset, final int length)
                throws IOException
        {
            try {
                int bytesRead = retry()
                        .maxAttempts(maxClientRetry)
                        .exponentialBackoff(new Duration(1, TimeUnit.SECONDS), maxBackoffTime, maxRetryTime, 2.0)
                        .stopOn(InterruptedException.class)
                        .run("readStream", () -> {
                            openStream();
                            try {
                                return in.read(buffer, offset, length);
                            }
                            catch (Exception e) {
                                closeStream();
                                throw e;
                            }
                        });

                if (bytesRead != -1) {
                    position += bytesRead;
                }
                return bytesRead;
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                Throwables.propagateIfInstanceOf(e, IOException.class);
                throw Throwables.propagate(e);
            }
        }

        @Override
        public boolean seekToNewSource(long targetPos)
                throws IOException
        {
            return false;
        }

        private S3Object getS3Object(final Path path, final long start)
                throws IOException
        {
            try {
                return retry()
                        .maxAttempts(maxClientRetry)
                        .exponentialBackoff(new Duration(1, TimeUnit.SECONDS), maxBackoffTime, maxRetryTime, 2.0)
                        .stopOn(InterruptedException.class, UnrecoverableS3OperationException.class)
                        .run("getS3Object", () -> {
                            try {
                                return s3.getObject(new GetObjectRequest(host, keyFromPath(path)).withRange(start, Long.MAX_VALUE));
                            }
                            catch (AmazonServiceException e) {
                                if (e.getStatusCode() == SC_FORBIDDEN) {
                                    throw new UnrecoverableS3OperationException(e);
                                }
                                throw Throwables.propagate(e);
                            }
                        });
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            catch (Exception e) {
                Throwables.propagateIfInstanceOf(e, IOException.class);
                throw Throwables.propagate(e);
            }
        }

        private void openStream()
                throws IOException
        {
            if (in == null) {
                in = getS3Object(path, position).getObjectContent();
            }
        }

        private void closeStream()
                throws IOException
        {
            if (in != null) {
                try {
                    in.abort();
                }
                catch (AbortedException ignored) {
                    // thrown if the current thread is in the interrupted state
                }
                in = null;
            }
        }
    }

    private static class PrestoS3OutputStream
            extends FilterOutputStream
    {
        private final TransferManager transferManager;
        private final String host;
        private final String key;
        private final File tempFile;

        private boolean closed;

        public PrestoS3OutputStream(AmazonS3 s3, TransferManagerConfiguration config, String host, String key, File tempFile)
                throws IOException
        {
            super(new BufferedOutputStream(new FileOutputStream(checkNotNull(tempFile, "tempFile is null"))));

            transferManager = new TransferManager(checkNotNull(s3, "s3 is null"));
            transferManager.setConfiguration(checkNotNull(config, "config is null"));

            this.host = checkNotNull(host, "host is null");
            this.key = checkNotNull(key, "key is null");
            this.tempFile = tempFile;

            log.debug("OutputStream for key '%s' using file: %s", key, tempFile);
        }

        @Override
        public void close()
                throws IOException
        {
            if (closed) {
                return;
            }
            closed = true;

            try {
                super.close();
                uploadObject();
            }
            finally {
                if (!tempFile.delete()) {
                    log.warn("Could not delete temporary file: %s", tempFile);
                }
                // close transfer manager but keep underlying S3 client open
                transferManager.shutdownNow(false);
            }
        }

        private void uploadObject()
                throws IOException
        {
            try {
                log.debug("Starting upload for host: %s, key: %s, file: %s, size: %s", host, key, tempFile, tempFile.length());
                Upload upload = transferManager.upload(host, key, tempFile);

                if (log.isDebugEnabled()) {
                    upload.addProgressListener(createProgressListener(upload));
                }

                upload.waitForCompletion();
                log.debug("Completed upload for host: %s, key: %s", host, key);
            }
            catch (AmazonClientException e) {
                throw new IOException(e);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException();
            }
        }

        private ProgressListener createProgressListener(final Transfer transfer)
        {
            return new ProgressListener()
            {
                private ProgressEventType previousType;
                private double previousTransferred;

                @Override
                public synchronized void progressChanged(ProgressEvent progressEvent)
                {
                    ProgressEventType eventType = progressEvent.getEventType();
                    if (previousType != eventType) {
                        log.debug("Upload progress event (%s/%s): %s", host, key, eventType);
                        previousType = eventType;
                    }

                    double transferred = transfer.getProgress().getPercentTransferred();
                    if (transferred >= (previousTransferred + 10.0)) {
                        log.debug("Upload percentage (%s/%s): %.0f%%", host, key, transferred);
                        previousTransferred = transferred;
                    }
                }
            };
        }
    }
}
