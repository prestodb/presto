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

import com.facebook.airlift.log.Logger;
import com.facebook.airlift.units.DataSize;
import com.facebook.airlift.units.Duration;
import com.facebook.presto.hive.filesystem.ExtendedFileSystem;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.AbstractSequentialIterator;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterators;
import com.google.common.io.Closer;
import com.google.common.net.MediaType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.BufferedFSInputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.auth.credentials.WebIdentityTokenFileCredentialsProvider;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.AbortedException;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.http.SdkHttpClient;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.services.s3.S3AsyncClientBuilder;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.CommonPrefix;
import software.amazon.awssdk.services.s3.model.CopyObjectRequest;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.ObjectCannedACL;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.S3Response;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.services.s3.model.StorageClass;
import software.amazon.awssdk.services.sts.StsClient;
import software.amazon.awssdk.services.sts.auth.StsAssumeRoleCredentialsProvider;
import software.amazon.awssdk.transfer.s3.S3TransferManager;
import software.amazon.awssdk.transfer.s3.model.CompletedFileUpload;
import software.amazon.awssdk.transfer.s3.model.FileUpload;
import software.amazon.awssdk.transfer.s3.model.UploadFileRequest;
import software.amazon.awssdk.transfer.s3.progress.TransferListener;
import software.amazon.encryption.s3.S3EncryptionClient;
import software.amazon.encryption.s3.materials.Keyring;
import software.amazon.encryption.s3.materials.KmsKeyring;

import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.EOFException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.file.Files;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.airlift.units.DataSize.Unit.MEGABYTE;
import static com.facebook.presto.hive.RetryDriver.retry;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_ACCESS_KEY;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_ACL_TYPE;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_CONNECT_TIMEOUT;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_CREDENTIALS_PROVIDER;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_ENCRYPTION_MATERIALS_PROVIDER;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_ENDPOINT;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_IAM_ROLE;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_IAM_ROLE_SESSION_NAME;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_KMS_KEY_ID;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_MAX_BACKOFF_TIME;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_MAX_CLIENT_RETRIES;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_MAX_CONNECTIONS;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_MAX_ERROR_RETRIES;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_MAX_RETRY_TIME;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_MULTIPART_MIN_FILE_SIZE;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_MULTIPART_MIN_PART_SIZE;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_PATH_STYLE_ACCESS;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_PIN_CLIENT_TO_CURRENT_REGION;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SECRET_KEY;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SIGNER_TYPE;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SKIP_GLACIER_OBJECTS;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SOCKET_TIMEOUT;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SSE_ENABLED;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SSE_KMS_KEY_ID;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SSE_TYPE;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_SSL_ENABLED;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_STAGING_DIRECTORY;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_STORAGE_CLASS;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_USER_AGENT_PREFIX;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_USER_AGENT_SUFFIX;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_USE_INSTANCE_CREDENTIALS;
import static com.facebook.presto.hive.s3.S3ConfigurationUpdater.S3_WEB_IDENTITY_ENABLED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkPositionIndexes;
import static com.google.common.base.Strings.isNullOrEmpty;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.base.Verify.verify;
import static com.google.common.collect.Iterables.toArray;
import static java.lang.Math.max;
import static java.lang.String.format;
import static java.net.HttpURLConnection.HTTP_BAD_REQUEST;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static java.net.HttpURLConnection.HTTP_NOT_FOUND;
import static java.nio.file.Files.createDirectories;
import static java.nio.file.Files.createTempFile;
import static java.time.Duration.ofMillis;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.hadoop.fs.FSExceptionMessages.NEGATIVE_SEEK;
import static org.apache.hadoop.fs.FSExceptionMessages.STREAM_IS_CLOSED;
import static software.amazon.awssdk.core.client.config.SdkAdvancedClientOption.USER_AGENT_PREFIX;
import static software.amazon.awssdk.services.s3.model.StorageClass.DEEP_ARCHIVE;
import static software.amazon.awssdk.services.s3.model.StorageClass.GLACIER;

public class PrestoS3FileSystem
        extends ExtendedFileSystem
{
    private static final Logger log = Logger.get(PrestoS3FileSystem.class);
    private static final PrestoS3FileSystemStats STATS = new PrestoS3FileSystemStats();
    private static volatile PrestoS3FileSystemMetricCollector metricCollector = new PrestoS3FileSystemMetricCollector(STATS);

    private static final String DIRECTORY_SUFFIX = "_$folder$";
    private static final DataSize BLOCK_SIZE = new DataSize(32, MEGABYTE);
    private static final DataSize MAX_SKIP_SIZE = new DataSize(1, MEGABYTE);
    private static final String PATH_SEPARATOR = "/";
    private static final Duration BACKOFF_MIN_SLEEP = new Duration(1, SECONDS);
    private static final int HTTP_RANGE_NOT_SATISFIABLE = 416;
    private static final MediaType X_DIRECTORY_MEDIA_TYPE = MediaType.create("application", "x-directory");
    private static final MediaType OCTET_STREAM_MEDIA_TYPE = MediaType.create("application", "octet-stream");
    private static final MediaType BINARY_OCTET_STREAM_MEDIA_TYPE = MediaType.create("binary", "octet-stream");
    private static final Set<String> GLACIER_STORAGE_CLASSES = ImmutableSet.of(
            GLACIER.toString(),
            DEEP_ARCHIVE.toString());

    private URI uri;
    private Path workingDirectory;
    private S3Client s3;
    private AwsCredentialsProvider credentialsProvider;
    private File stagingDirectory;
    private int maxAttempts;
    private Duration maxBackoffTime;
    private Duration maxRetryTime;
    private boolean useInstanceCredentials;
    private String s3IamRole;
    private String s3IamRoleSessionName;
    private boolean pinS3ClientToCurrentRegion;
    private boolean sseEnabled;
    private PrestoS3SseType sseType;
    private String sseKmsKeyId;
    private boolean isPathStyleAccess;
    private long multiPartUploadMinFileSize;
    private long multiPartUploadMinPartSize;
    private PrestoS3AclType s3AclType;
    private boolean skipGlacierObjects;
    private PrestoS3StorageClass s3StorageClass;
    private boolean webIdentityEnabled;

    @Override
    public void initialize(URI uri, Configuration conf)
            throws IOException
    {
        requireNonNull(uri, "uri is null");
        requireNonNull(conf, "conf is null");

        super.initialize(uri, conf);
        setConf(conf);

        this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
        this.workingDirectory = new Path(PATH_SEPARATOR).makeQualified(this.uri, new Path(PATH_SEPARATOR));

        HiveS3Config defaults = new HiveS3Config();
        this.stagingDirectory = new File(conf.get(S3_STAGING_DIRECTORY, defaults.getS3StagingDirectory().toString()));
        this.maxAttempts = conf.getInt(S3_MAX_CLIENT_RETRIES, defaults.getS3MaxClientRetries()) + 1;
        this.maxBackoffTime = Duration.valueOf(conf.get(S3_MAX_BACKOFF_TIME, defaults.getS3MaxBackoffTime().toString()));
        this.maxRetryTime = Duration.valueOf(conf.get(S3_MAX_RETRY_TIME, defaults.getS3MaxRetryTime().toString()));

        this.multiPartUploadMinFileSize = conf.getLong(S3_MULTIPART_MIN_FILE_SIZE, defaults.getS3MultipartMinFileSize().toBytes());
        this.multiPartUploadMinPartSize = conf.getLong(S3_MULTIPART_MIN_PART_SIZE, defaults.getS3MultipartMinPartSize().toBytes());
        this.isPathStyleAccess = conf.getBoolean(S3_PATH_STYLE_ACCESS, defaults.isS3PathStyleAccess());
        this.useInstanceCredentials = conf.getBoolean(S3_USE_INSTANCE_CREDENTIALS, defaults.isS3UseInstanceCredentials());
        this.pinS3ClientToCurrentRegion = conf.getBoolean(S3_PIN_CLIENT_TO_CURRENT_REGION, defaults.isPinS3ClientToCurrentRegion());
        this.s3IamRole = conf.get(S3_IAM_ROLE, defaults.getS3IamRole());
        this.s3IamRoleSessionName = conf.get(S3_IAM_ROLE_SESSION_NAME, defaults.getS3IamRoleSessionName());

        verify(!(useInstanceCredentials && conf.get(S3_IAM_ROLE) != null),
                "Invalid configuration: either use instance credentials or specify an iam role");
        verify((pinS3ClientToCurrentRegion && conf.get(S3_ENDPOINT) == null) || !pinS3ClientToCurrentRegion,
                "Invalid configuration: either endpoint can be set or S3 client can be pinned to the current region");

        this.sseEnabled = conf.getBoolean(S3_SSE_ENABLED, defaults.isS3SseEnabled());
        this.sseType = PrestoS3SseType.valueOf(conf.get(S3_SSE_TYPE, defaults.getS3SseType().name()));
        this.sseKmsKeyId = conf.get(S3_SSE_KMS_KEY_ID, defaults.getS3SseKmsKeyId());

        this.s3AclType = PrestoS3AclType.valueOf(conf.get(S3_ACL_TYPE, defaults.getS3AclType().name()));
        this.skipGlacierObjects = conf.getBoolean(S3_SKIP_GLACIER_OBJECTS, defaults.isSkipGlacierObjects());
        this.s3StorageClass = conf.getEnum(S3_STORAGE_CLASS, defaults.getS3StorageClass());
        this.webIdentityEnabled = conf.getBoolean(S3_WEB_IDENTITY_ENABLED, false);

        checkArgument(!(webIdentityEnabled && isNullOrEmpty(s3IamRole)),
                "Invalid configuration: hive.s3.iam-role must be provided when hive.s3.web.identity.auth.enabled is set to true");

        this.credentialsProvider = createAwsCredentialsProvider(uri, conf);
        this.s3 = createS3Client(conf);
    }

    @Override
    public void close()
            throws IOException
    {
        try (Closer closer = Closer.create()) {
            closer.register(super::close);
            if (credentialsProvider instanceof Closeable) {
                closer.register((Closeable) credentialsProvider);
            }
            closer.register(s3::close);
        }
    }

    @Override
    public URI getUri()
    {
        return uri;
    }

    @Override
    public String getScheme()
    {
        return uri.getScheme();
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
        STATS.newListStatusCall();
        List<LocatedFileStatus> list = new ArrayList<>();
        RemoteIterator<LocatedFileStatus> iterator = listLocatedStatus(path);
        while (iterator.hasNext()) {
            list.add(iterator.next());
        }
        return toArray(list, LocatedFileStatus.class);
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listFiles(Path path, boolean recursive)
    {
        // Either a single level or full listing, depending on the recursive flag, no "directories" are included
        return new S3ObjectsRemoteIterator(listPrefix(path, OptionalInt.empty(), recursive ? ListingMode.RECURSIVE_FILES_ONLY : ListingMode.SHALLOW_FILES_ONLY));
    }

    @Override
    public RemoteIterator<LocatedFileStatus> listLocatedStatus(Path path)
    {
        STATS.newListLocatedStatusCall();
        return new S3ObjectsRemoteIterator(listPrefix(path, OptionalInt.empty(), ListingMode.SHALLOW_ALL));
    }

    @Override
    public FileStatus getFileStatus(Path path)
            throws IOException
    {
        if (path.getName().isEmpty()) {
            // the bucket root requires special handling
            if (getS3ObjectMetadata(path).getObjectResponse() != null) {
                return new FileStatus(0, true, 1, 0, 0, qualifiedPath(path));
            }
            throw new FileNotFoundException("File does not exist: " + path);
        }

        PrestoS3ObjectMetadata metadata = getS3ObjectMetadata(path);

        if (metadata.getObjectResponse() == null) {
            // check if this path is a directory
            Iterator<LocatedFileStatus> iterator = listPrefix(path, OptionalInt.of(1), ListingMode.SHALLOW_ALL);
            if (iterator.hasNext()) {
                return new FileStatus(0, true, 1, 0, 0, qualifiedPath(path));
            }
            throw new FileNotFoundException("File does not exist: " + path);
        }

        checkArgument(metadata.getObjectResponse() instanceof HeadObjectResponse);

        return new FileStatus(
                // Some directories (e.g. uploaded through S3 GUI) return a charset in the Content-Type header
                ((HeadObjectResponse) metadata.getObjectResponse()).contentLength(),
                isDirectory(metadata),
                1,
                BLOCK_SIZE.toBytes(),
                lastModifiedTime((HeadObjectResponse) metadata.getObjectResponse()),
                qualifiedPath(path));
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize)
    {
        return new FSDataInputStream(
                new BufferedFSInputStream(
                        new PrestoS3InputStream(s3, getBucketName(uri), path, maxAttempts, maxBackoffTime, maxRetryTime),
                        bufferSize));
    }

    @Override
    public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite,
                                     int bufferSize, short replication, long blockSize, Progressable progress)
            throws IOException
    {
        if ((!overwrite) && exists(path)) {
            throw new IOException("File already exists:" + path);
        }

        if (!stagingDirectory.exists()) {
            createDirectories(stagingDirectory.toPath());
        }
        if (!stagingDirectory.isDirectory()) {
            throw new IOException("Configured staging path is not a directory: " + stagingDirectory);
        }
        File tempFile = createTempFile(stagingDirectory.toPath(), "presto-s3-", ".tmp").toFile();

        String key = keyFromPath(qualifiedPath(path));

        Region region = Region.US_EAST_1;
        if (pinS3ClientToCurrentRegion) {
            try {
                Region currentRegion = new DefaultAwsRegionProviderChain().getRegion();
                if (currentRegion != null) {
                    region = currentRegion;
                }
            }
            catch (Exception ignored) { }
        }

        Configuration conf = getConf();
        String endpoint = conf.get(S3_ENDPOINT);

        boolean disableChecksums = false;
        if (endpoint != null) {
            try {
                URI endpointUri = URI.create(endpoint);
                if (endpointUri.getScheme() == null) {
                    boolean sslEnabled = conf.getBoolean(S3_SSL_ENABLED, true);
                    endpoint = (sslEnabled ? "https://" : "http://") + endpoint;
                    endpointUri = URI.create(endpoint);
                }
                disableChecksums = "http".equalsIgnoreCase(endpointUri.getScheme());
                if (disableChecksums) {
                    log.debug("HTTP endpoint detected for uploads: %s - disabling checksum validation", endpoint);
                }
            }
            catch (IllegalArgumentException e) {
                log.error("Invalid S3 endpoint URL: %s", endpoint);
            }
        }

        return new FSDataOutputStream(
                new PrestoS3OutputStream(
                        s3,
                        credentialsProvider,
                        region,
                        endpoint,
                        isPathStyleAccess,
                        getBucketName(uri),
                        key,
                        tempFile,
                        sseEnabled,
                        sseType,
                        sseKmsKeyId,
                        multiPartUploadMinFileSize,
                        multiPartUploadMinPartSize,
                        s3AclType,
                        s3StorageClass,
                        disableChecksums),
                statistics);
    }

    @Override
    public FSDataOutputStream append(Path f, int bufferSize, Progressable progress)
    {
        throw new UnsupportedOperationException("append");
    }

    @Override
    public boolean rename(Path src, Path dst)
            throws IOException
    {
        boolean srcDirectory;
        try {
            srcDirectory = directory(src);
        }
        catch (FileNotFoundException e) {
            return false;
        }

        try {
            if (!directory(dst)) {
                // cannot copy a file to an existing file
                return false;
            }
            // move source under destination directory
            dst = new Path(dst, src.getName());
        }
        catch (FileNotFoundException e) {
            // destination does not exist
        }

        if (keysEqual(src, dst)) {
            return false;
        }

        if (srcDirectory) {
            for (FileStatus file : listStatus(src)) {
                rename(file.getPath(), new Path(dst, file.getPath().getName()));
            }
            deleteObject(keyFromPath(src) + DIRECTORY_SUFFIX);
        }
        else {
            CopyObjectRequest copyRequest = CopyObjectRequest.builder()
                    .sourceBucket(getBucketName(uri))
                    .sourceKey(keyFromPath(src))
                    .destinationBucket(getBucketName(uri))
                    .destinationKey(keyFromPath(dst))
                    .build();
            s3.copyObject(copyRequest);
            delete(src, true);
        }

        return true;
    }

    @Override
    public boolean delete(Path path, boolean recursive)
            throws IOException
    {
        try {
            if (!directory(path)) {
                return deleteObject(keyFromPath(path));
            }
        }
        catch (FileNotFoundException e) {
            return false;
        }

        if (!recursive) {
            throw new IOException("Directory " + path + " is not empty");
        }

        for (FileStatus file : listStatus(path)) {
            delete(file.getPath(), true);
        }
        deleteObject(keyFromPath(path) + DIRECTORY_SUFFIX);

        return true;
    }

    @Override
    public boolean mkdirs(Path f, FsPermission permission)
    {
        // no need to do anything for S3
        return true;
    }

    private boolean directory(Path path)
            throws IOException
    {
        return getFileStatus(path).isDirectory();
    }

    private boolean deleteObject(String key)
    {
        try {
            DeleteObjectRequest deleteRequest = DeleteObjectRequest.builder()
                    .bucket(getBucketName(uri))
                    .key(key)
                    .build();
            s3.deleteObject(deleteRequest);
            return true;
        }
        catch (SdkClientException e) {
            return false;
        }
    }

    private enum ListingMode {
        SHALLOW_ALL, // Shallow listing of files AND directories
        SHALLOW_FILES_ONLY,
        RECURSIVE_FILES_ONLY;

        public boolean isFilesOnly()
        {
            return (this == SHALLOW_FILES_ONLY || this == RECURSIVE_FILES_ONLY);
        }
    }

    private Iterator<LocatedFileStatus> listPrefix(Path path, OptionalInt initialMaxKeys, ListingMode mode)
    {
        String key = keyFromPath(path);
        if (!key.isEmpty()) {
            key += PATH_SEPARATOR;
        }

        ListObjectsV2Request.Builder requestBuilder = ListObjectsV2Request.builder()
                .bucket(getBucketName(uri))
                .prefix(key)
                .delimiter(mode == ListingMode.RECURSIVE_FILES_ONLY ? null : PATH_SEPARATOR);

        if (initialMaxKeys.isPresent()) {
            requestBuilder.maxKeys(initialMaxKeys.getAsInt());
        }

        ListObjectsV2Request request = requestBuilder.build();

        STATS.newListObjectsCall();
        Iterator<ListObjectsV2Response> listings = new AbstractSequentialIterator<ListObjectsV2Response>(s3.listObjectsV2(request))
        {
            @Override
            protected ListObjectsV2Response computeNext(ListObjectsV2Response previous)
            {
                if (!previous.isTruncated()) {
                    return null;
                }
                // Clear any max keys set initially to allow AWS S3 to use its default batch size.
                //Use the ContinuationToken from the previous response to fetch the next set of objects.
                ListObjectsV2Request nextRequest = request.toBuilder()
                        .maxKeys(null)
                        .continuationToken(previous.nextContinuationToken())
                        .build();
                return s3.listObjectsV2(nextRequest);
            }
        };

        Iterator<LocatedFileStatus> result = Iterators.concat(Iterators.transform(listings, this::statusFromListing));
        if (mode.isFilesOnly()) {
            //  Even recursive listing can still contain empty "directory" objects, must filter them out
            result = Iterators.filter(result, LocatedFileStatus::isFile);
        }
        return result;
    }

    private Iterator<LocatedFileStatus> statusFromListing(ListObjectsV2Response listing)
    {
        List<String> prefixes = new ArrayList<>();
        for (CommonPrefix commonPrefix : listing.commonPrefixes()) {
            prefixes.add(commonPrefix.prefix());
        }

        List<S3Object> objects = listing.contents();
        if (prefixes.isEmpty()) {
            return statusFromObjects(objects);
        }
        if (objects.isEmpty()) {
            return statusFromPrefixes(prefixes);
        }
        return Iterators.concat(
                statusFromPrefixes(prefixes),
                statusFromObjects(objects));
    }

    private Iterator<LocatedFileStatus> statusFromPrefixes(List<String> prefixes)
    {
        List<LocatedFileStatus> list = new ArrayList<>(prefixes.size());
        for (String prefix : prefixes) {
            Path path = qualifiedPath(new Path(PATH_SEPARATOR + prefix));
            FileStatus status = new FileStatus(0, true, 1, 0, 0, path);
            list.add(createLocatedFileStatus(status));
        }
        return list.iterator();
    }

    private Iterator<LocatedFileStatus> statusFromObjects(List<S3Object> objects)
    {
        // NOTE: for encrypted objects, S3Object.size() used below is NOT correct,
        // however, to get the correct size we'd need to make an additional request to get
        // user metadata, and in this case it doesn't matter.
        return objects.stream()
                .filter(object -> !object.key().endsWith(PATH_SEPARATOR))
                .filter(object -> !skipGlacierObjects || !isGlacierObject(object))
                .filter(object -> !isHadoopFolderMarker(object))
                .map(object -> new FileStatus(
                        object.size(),
                        false,
                        1,
                        BLOCK_SIZE.toBytes(),
                        object.lastModified().toEpochMilli(),
                        qualifiedPath(new Path(PATH_SEPARATOR + object.key()))))
                .map(this::createLocatedFileStatus)
                .iterator();
    }

    private boolean isGlacierObject(S3Object object)
    {
        return GLACIER_STORAGE_CLASSES.contains(object.storageClassAsString());
    }

    private boolean isHadoopFolderMarker(S3Object object)
    {
        return (object.key().endsWith(DIRECTORY_SUFFIX) && (object.size() == 0));
    }

    private static boolean isDirectory(PrestoS3ObjectMetadata metadata)
    {
        HeadObjectResponse response = (HeadObjectResponse) metadata.getObjectResponse();
        String contentType = response.contentType();

        MediaType mediaType;
        try {
            mediaType = MediaType.parse(contentType);
        }
        catch (IllegalArgumentException e) {
            log.debug(e, "Failed to parse contentType [%s], assuming not a directory", contentType);
            return false;
        }

        return mediaType.is(X_DIRECTORY_MEDIA_TYPE) ||
                ((mediaType.is(OCTET_STREAM_MEDIA_TYPE) || mediaType.is(BINARY_OCTET_STREAM_MEDIA_TYPE))
                        && metadata.isKeyNeedsPathSeparator()
                        && response.contentLength() == 0);
    }

    @VisibleForTesting
    PrestoS3ObjectMetadata getS3ObjectMetadata(Path path)
            throws IOException
    {
        String bucketName = getBucketName(uri);
        String key = keyFromPath(path);
        S3Response s3ObjectResponse = getS3ObjectMetadata(path, bucketName, key);
        if (s3ObjectResponse == null && !key.isEmpty()) {
            return new PrestoS3ObjectMetadata(getS3ObjectMetadata(path, bucketName, key + PATH_SEPARATOR), true);
        }
        return new PrestoS3ObjectMetadata(s3ObjectResponse, false);
    }

    private S3Response getS3ObjectMetadata(Path path, String bucketName, String key)
            throws IOException
    {
        try {
            return retry()
                    .maxAttempts(maxAttempts)
                    .exponentialBackoff(BACKOFF_MIN_SLEEP, maxBackoffTime, maxRetryTime, 2.0)
                    .stopOn(InterruptedException.class, UnrecoverableS3OperationException.class, AbortedException.class)
                    .onRetry(STATS::newGetMetadataRetry)
                    .run("getS3ObjectMetadata", () -> {
                        try {
                            STATS.newMetadataCall();

                            if (key.isEmpty()) {
                                HeadBucketRequest request = HeadBucketRequest.builder()
                                        .bucket(bucketName)
                                        .build();
                                return s3.headBucket(request);
                            }

                            HeadObjectRequest request = HeadObjectRequest.builder()
                                    .bucket(bucketName)
                                    .key(key)
                                    .build();
                            return s3.headObject(request);
                        }
                        catch (RuntimeException e) {
                            STATS.newGetMetadataError();
                            if (e instanceof S3Exception) {
                                S3Exception s3Exception = (S3Exception) e;
                                switch (s3Exception.statusCode()) {
                                    case HTTP_NOT_FOUND:
                                        return null;
                                    case HTTP_FORBIDDEN:
                                    case HTTP_BAD_REQUEST:
                                        throw new UnrecoverableS3OperationException(path, e);
                                }
                            }
                            throw e;
                        }
                    });
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        catch (Exception e) {
            throwIfInstanceOf(e, IOException.class);
            throwIfUnchecked(e);
            throw new RuntimeException(e);
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
            throw new UncheckedIOException(e);
        }
    }

    private static long lastModifiedTime(HeadObjectResponse response)
    {
        Instant lastModified = response.lastModified();
        return (lastModified != null) ? lastModified.toEpochMilli() : 0;
    }

    private static boolean keysEqual(Path p1, Path p2)
    {
        return keyFromPath(p1).equals(keyFromPath(p2));
    }

    public static String keyFromPath(Path path)
    {
        checkArgument(path.isAbsolute(), "Path is not absolute: %s", path);
        String key = nullToEmpty(path.toUri().getPath());
        if (key.startsWith(PATH_SEPARATOR)) {
            key = key.substring(PATH_SEPARATOR.length());
        }
        if (key.endsWith(PATH_SEPARATOR)) {
            key = key.substring(0, key.length() - PATH_SEPARATOR.length());
        }
        return key;
    }

    private S3Client createS3Client(Configuration hadoopConfig)
    {
        HiveS3Config defaults = new HiveS3Config();

        Duration connectTimeout = Duration.valueOf(hadoopConfig.get(S3_CONNECT_TIMEOUT, defaults.getS3ConnectTimeout().toString()));
        Duration socketTimeout = Duration.valueOf(hadoopConfig.get(S3_SOCKET_TIMEOUT, defaults.getS3SocketTimeout().toString()));
        int maxConnections = hadoopConfig.getInt(S3_MAX_CONNECTIONS, defaults.getS3MaxConnections());
        int maxErrorRetries = hadoopConfig.getInt(S3_MAX_ERROR_RETRIES, defaults.getS3MaxErrorRetries());
        boolean sslEnabled = hadoopConfig.getBoolean(S3_SSL_ENABLED, defaults.isS3SslEnabled());
        String userAgentPrefix = hadoopConfig.get(S3_USER_AGENT_PREFIX, defaults.getS3UserAgentPrefix());

        ApacheHttpClient.Builder httpClientBuilder = ApacheHttpClient.builder()
                .maxConnections(maxConnections)
                .connectionTimeout(ofMillis(connectTimeout.toMillis()))
                .socketTimeout(ofMillis(socketTimeout.toMillis()));

        // Configure SSL/TLS settings if needed
        if (!sslEnabled) {
            log.warn("SSL is disabled - this is not recommended for production use");
        }

        String endpoint = hadoopConfig.get(S3_ENDPOINT);
        boolean isHttpEndpoint = false;

        // Check if endpoint is HTTP (non-secure)
        if (endpoint != null) {
            try {
                URI endpointUri = URI.create(endpoint);
                if (endpointUri.getScheme() == null) {
                    endpoint = (sslEnabled ? "https://" : "http://") + endpoint;
                    endpointUri = URI.create(endpoint);
                }
                isHttpEndpoint = "http".equalsIgnoreCase(endpointUri.getScheme());
                if (isHttpEndpoint) {
                    log.debug("HTTP endpoint detected: %s - will disable checksum validation", endpoint);
                }
            }
            catch (IllegalArgumentException e) {
                log.error("Invalid S3 endpoint URL: %s", endpoint);
                throw new RuntimeException("Invalid S3 endpoint configuration", e);
            }
        }

        // **FIX: Build S3Configuration with checksum validation disabled for HTTP endpoints**
        // This prevents the x-amz-content-sha256 mismatch error when using HTTP endpoints
        // See: https://github.com/aws/aws-sdk-java-v2/issues/5498
        final boolean disableChecksums = isHttpEndpoint;
        S3Configuration s3Configuration = S3Configuration.builder()
                .checksumValidationEnabled(!disableChecksums)
                .build();

        Optional<Keyring> keyring = createClientSideEncryptionKeyring(hadoopConfig);
        if (keyring.isPresent()) {
            log.debug("Creating S3 client with client-side encryption");
            return createS3EncryptionClient(hadoopConfig, httpClientBuilder.build(),
                    s3Configuration, keyring.get());
        }

        S3ClientBuilder clientBuilder = S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .httpClient(httpClientBuilder.build())
                .serviceConfiguration(s3Configuration)
                .overrideConfiguration(builder -> {
                    builder.retryStrategy(retryStrategy -> retryStrategy.maxAttempts(maxErrorRetries))
                            .putAdvancedOption(USER_AGENT_PREFIX,
                                    userAgentPrefix + " " + S3_USER_AGENT_SUFFIX);

                    // Handle signer override if specified
                    String signerType = hadoopConfig.get(S3_SIGNER_TYPE);
                    if (signerType != null) {
                        log.debug("Signer type configuration: %s (Note: v2 handles signers differently)", signerType);
                    }
                });

        boolean regionOrEndpointSet = false;

        // Use local region when running inside of EC2
        if (pinS3ClientToCurrentRegion) {
            try {
                Region region = new DefaultAwsRegionProviderChain().getRegion();
                if (region != null) {
                    clientBuilder.region(region);
                    regionOrEndpointSet = true;
                    log.debug("Using region from provider chain: %s", region);
                }
            }
            catch (Exception e) {
                log.debug("Could not determine current region from provider chain: %s", e.getMessage());
            }
        }

        if (endpoint != null) {
            clientBuilder.endpointOverride(URI.create(endpoint));

            // Defaulting to the us-east-1 region.
            // In AWS SDK V1, Presto would automatically use us-east-1 if no region was specified.
            // However, AWS SDK V2 determines the region using the DefaultAwsRegionProviderChain,
            // which may not be available when Presto is not running on EC2.
            clientBuilder.region(Region.US_EAST_1);

            log.debug("Using custom endpoint: %s", endpoint);
            regionOrEndpointSet = true;
        }

        if (isPathStyleAccess) {
            clientBuilder.forcePathStyle(true);
            log.debug("Using path-style access");
        }

        if (!regionOrEndpointSet) {
            clientBuilder.region(Region.US_EAST_1);
            clientBuilder.crossRegionAccessEnabled(true);
            log.debug("No region or endpoint specified, defaulting to US_EAST_1");
        }

        return clientBuilder.build();
    }

    private Region determineKmsRegion(Configuration conf)
    {
        String kmsRegion = conf.get("hive.s3.kms.region");
        if (!isNullOrEmpty(kmsRegion)) {
            return Region.of(kmsRegion);
        }
        if (pinS3ClientToCurrentRegion) {
            try {
                Region region = new DefaultAwsRegionProviderChain().getRegion();
                if (region != null) {
                    return region;
                }
            }
            catch (Exception ignored) { }
        }
        return Region.US_EAST_1;
    }

    private Optional<Keyring> createClientSideEncryptionKeyring(Configuration hadoopConfig)
    {
        String kmsKeyId = hadoopConfig.get(S3_KMS_KEY_ID);
        if (kmsKeyId != null) {
            Region kmsRegion = determineKmsRegion(hadoopConfig);
            KmsClient kmsClient = KmsClient.builder()
                    .credentialsProvider(credentialsProvider)
                    .region(kmsRegion)
                    .build();

            return Optional.of(KmsKeyring.builder()
                    .kmsClient(kmsClient)
                    .wrappingKeyId(kmsKeyId)
                    .build());
        }

        String empClassName = hadoopConfig.get(S3_ENCRYPTION_MATERIALS_PROVIDER);
        if (empClassName != null) {
            log.warn("Custom encryption materials provider from v1 (%s) needs to be reimplemented as a Keyring for v2/v3",
                    empClassName);
            throw new UnsupportedOperationException(
                    "Custom encryption materials providers must be migrated to Keyring interface");
        }

        return Optional.empty();
    }

    private S3Client createS3EncryptionClient(
            Configuration hadoopConfig,
            SdkHttpClient httpClient,
            S3Configuration s3Configuration,
            Keyring keyring)
    {
        S3ClientBuilder baseClientBuilder = S3Client.builder()
                .credentialsProvider(credentialsProvider)
                .httpClient(httpClient)
                .serviceConfiguration(s3Configuration)
                .overrideConfiguration(builder -> {
                    builder.retryStrategy(retryStrategy ->
                                    retryStrategy.maxAttempts(hadoopConfig.getInt(S3_MAX_ERROR_RETRIES, 3)))
                            .putAdvancedOption(USER_AGENT_PREFIX,
                                    hadoopConfig.get(S3_USER_AGENT_PREFIX, "") + " " + S3_USER_AGENT_SUFFIX);
                });

        String endpoint = hadoopConfig.get(S3_ENDPOINT);
        if (endpoint != null) {
            baseClientBuilder.endpointOverride(URI.create(endpoint));
            baseClientBuilder.region(Region.US_EAST_1);
        }
        else if (pinS3ClientToCurrentRegion) {
            try {
                Region region = new DefaultAwsRegionProviderChain().getRegion();
                if (region != null) {
                    baseClientBuilder.region(region);
                }
            }
            catch (Exception e) {
                log.debug("Could not determine region: %s", e.getMessage());
                baseClientBuilder.region(Region.US_EAST_1);
            }
        }
        else {
            baseClientBuilder.region(Region.US_EAST_1);
            baseClientBuilder.crossRegionAccessEnabled(true);
        }

        if (isPathStyleAccess) {
            baseClientBuilder.forcePathStyle(true);
        }

        S3Client baseClient = baseClientBuilder.build();
        return S3EncryptionClient.builder()
                .wrappedClient(baseClient)
                .keyring(keyring)
                .enableLegacyUnauthenticatedModes(false)
                .build();
    }

    private AwsCredentialsProvider createAwsCredentialsProvider(URI uri, Configuration conf)
    {
        Optional<AwsCredentials> credentials = getAwsCredentials(uri, conf);
        if (credentials.isPresent()) {
            return StaticCredentialsProvider.create(credentials.get());
        }

        if (useInstanceCredentials) {
            return InstanceProfileCredentialsProvider.create();
        }

        if (!isNullOrEmpty(s3IamRole)) {
            if (webIdentityEnabled) {
                log.debug("Using Web Identity Token Credentials Provider.");
                return WebIdentityTokenFileCredentialsProvider.builder()
                        .roleArn(s3IamRole)
                        .roleSessionName(s3IamRoleSessionName)
                        .build();
            }
            log.debug("Using STS Assume Role Session Credentials Provider.");
            StsClient stsClient = StsClient.builder()
                    .credentialsProvider(DefaultCredentialsProvider.create())
                    .region(Region.US_EAST_1)
                    .build();
            return StsAssumeRoleCredentialsProvider.builder()
                    .refreshRequest(request -> request
                            .roleArn(s3IamRole)
                            .roleSessionName(s3IamRoleSessionName))
                    .stsClient(stsClient)
                    .build();
        }

        String providerClass = conf.get(S3_CREDENTIALS_PROVIDER);
        if (!isNullOrEmpty(providerClass)) {
            return getCustomAWSCredentialsProvider(uri, conf, providerClass);
        }

        return DefaultCredentialsProvider.create();
    }

    private static AwsCredentialsProvider getCustomAWSCredentialsProvider(URI uri, Configuration conf, String providerClass)
    {
        try {
            log.debug("Using AWS credential provider %s for URI %s", providerClass, uri);
            return conf.getClassByName(providerClass)
                    .asSubclass(AwsCredentialsProvider.class)
                    .getConstructor(URI.class, Configuration.class)
                    .newInstance(uri, conf);
        }
        catch (ReflectiveOperationException e) {
            throw new RuntimeException(format("Error creating an instance of %s for URI %s", providerClass, uri), e);
        }
    }

    private static Optional<AwsCredentials> getAwsCredentials(URI uri, Configuration conf)
    {
        String accessKey = conf.get(S3_ACCESS_KEY);
        String secretKey = conf.get(S3_SECRET_KEY);

        String userInfo = uri.getUserInfo();
        if (userInfo != null) {
            int index = userInfo.indexOf(':');
            if (index < 0) {
                accessKey = userInfo;
            }
            else {
                accessKey = userInfo.substring(0, index);
                secretKey = userInfo.substring(index + 1);
            }
        }

        if (isNullOrEmpty(accessKey) || isNullOrEmpty(secretKey)) {
            return Optional.empty();
        }
        return Optional.of(AwsBasicCredentials.create(accessKey, secretKey));
    }

    /**
     * This exception is for stopping retries for S3 calls that shouldn't be retried.
     * For example, "Caused by: com.amazonaws.services.s3.model.AmazonS3Exception: Forbidden (Service: Amazon S3; Status Code: 403 ..."
     */

    @VisibleForTesting
    static class UnrecoverableS3OperationException
            extends IOException
    {
        public UnrecoverableS3OperationException(Path path, Throwable cause)
        {
            // append the path info to the message
            super(format("%s (Path: %s)", cause, path), cause);
        }
    }

    public static class PrestoS3ObjectMetadata
    {
        /**
         * Certain filesystems treat empty directories as zero byte objects and their name ends with a path separator, i.e. '/'.
         * To fetch ObjectMetadata for such keys, the path separator needs to be appended to the key otherwise null is returned.
         * This field denotes whether a path separator was appended to the key while fetching the metadata for given path.
         */
        private final S3Response objectResponse;
        private final boolean keyNeedsPathSeparator;

        public PrestoS3ObjectMetadata(S3Response objectResponse, boolean keyNeedsPathSeparator)
        {
            this.objectResponse = objectResponse;
            this.keyNeedsPathSeparator = keyNeedsPathSeparator;
        }

        public S3Response getObjectResponse()
        {
            return objectResponse;
        }

        public boolean isKeyNeedsPathSeparator()
        {
            return keyNeedsPathSeparator;
        }
    }

    private static final class S3ObjectsRemoteIterator
            implements RemoteIterator<LocatedFileStatus>
    {
        private final Iterator<LocatedFileStatus> iterator;

        public S3ObjectsRemoteIterator(Iterator<LocatedFileStatus> iterator)
        {
            this.iterator = requireNonNull(iterator, "iterator is null");
        }

        @Override
        public boolean hasNext()
                throws IOException
        {
            try {
                return iterator.hasNext();
            }
            catch (SdkClientException e) {
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
            catch (SdkClientException e) {
                throw new IOException(e);
            }
        }
    }

    private static class PrestoS3InputStream
            extends FSInputStream
    {
        private final S3Client s3;
        private final String host;
        private final Path path;
        private final int maxAttempts;
        private final Duration maxBackoffTime;
        private final Duration maxRetryTime;
        private final AtomicBoolean closed = new AtomicBoolean();

        private InputStream in;
        private long streamPosition;
        private long nextReadPosition;

        public PrestoS3InputStream(S3Client s3, String host, Path path, int maxAttempts, Duration maxBackoffTime, Duration maxRetryTime)
        {
            this.s3 = requireNonNull(s3, "s3 is null");
            this.host = requireNonNull(host, "host is null");
            this.path = requireNonNull(path, "path is null");

            checkArgument(maxAttempts >= 0, "maxAttempts cannot be negative");
            this.maxAttempts = maxAttempts;
            this.maxBackoffTime = requireNonNull(maxBackoffTime, "maxBackoffTime is null");
            this.maxRetryTime = requireNonNull(maxRetryTime, "maxRetryTime is null");
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
                        .stopOn(InterruptedException.class, UnrecoverableS3OperationException.class, EOFException.class, FileNotFoundException.class, AbortedException.class)
                        .onRetry(STATS::newGetObjectRetry)
                        .run("getS3Object", () -> {
                            InputStream stream;
                            try {
                                GetObjectRequest request = GetObjectRequest.builder()
                                        .bucket(host)
                                        .key(keyFromPath(path))
                                        .range("bytes=" + position + "-" + ((position + length) - 1))
                                        .build();
                                ResponseInputStream<GetObjectResponse> responseStream = s3.getObject(request);
                                stream = responseStream;
                            }
                            catch (RuntimeException e) {
                                STATS.newGetObjectError();
                                if (e instanceof S3Exception) {
                                    S3Exception s3Exception = (S3Exception) e;
                                    switch (s3Exception.statusCode()) {
                                        case HTTP_RANGE_NOT_SATISFIABLE:
                                            return -1;
                                        case HTTP_NOT_FOUND:
                                            throw new FileNotFoundException("File does not exist: " + path);
                                        case HTTP_FORBIDDEN:
                                        case HTTP_BAD_REQUEST:
                                            throw new UnrecoverableS3OperationException(path, e);
                                    }
                                }
                                throw e;
                            }

                            STATS.connectionOpened();
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
                                STATS.newReadError(t);
                                abortStream(stream);
                                throw t;
                            }
                            finally {
                                STATS.connectionReleased();
                                stream.close();
                            }
                        });
            }
            catch (Exception e) {
                throw propagate(e);
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
            // This stream is wrapped with BufferedInputStream, so this method should never be called
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
                        .stopOn(InterruptedException.class, UnrecoverableS3OperationException.class, AbortedException.class, FileNotFoundException.class)
                        .onRetry(STATS::newReadRetry)
                        .run("readStream", () -> {
                            seekStream();
                            try {
                                return in.read(buffer, offset, length);
                            }
                            catch (Exception e) {
                                STATS.newReadError(e);
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
                if (skip <= max(in.available(), MAX_SKIP_SIZE.toBytes())) {
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
                STATS.connectionOpened();
            }
        }

        private InputStream openStream(Path path, long start)
                throws IOException
        {
            try {
                return retry()
                        .maxAttempts(maxAttempts)
                        .exponentialBackoff(BACKOFF_MIN_SLEEP, maxBackoffTime, maxRetryTime, 2.0)
                        .stopOn(InterruptedException.class, UnrecoverableS3OperationException.class, FileNotFoundException.class, AbortedException.class, EOFException.class)
                        .onRetry(STATS::newGetObjectRetry)
                        .run("getS3Object", () -> {
                            try {
                                GetObjectRequest request = GetObjectRequest.builder()
                                        .bucket(host)
                                        .key(keyFromPath(path))
                                        .range("bytes=" + start + "-")
                                        .build();
                                return s3.getObject(request);
                            }
                            catch (RuntimeException e) {
                                STATS.newGetObjectError();
                                if (e instanceof S3Exception) {
                                    S3Exception s3Exception = (S3Exception) e;
                                    switch (s3Exception.statusCode()) {
                                        case HTTP_RANGE_NOT_SATISFIABLE:
                                            // Return an empty stream instead of throwing EOFException
                                            // This allows read() to return -1 indicating end of stream
                                            return new ByteArrayInputStream(new byte[0]);
                                        case HTTP_NOT_FOUND:
                                            throw new FileNotFoundException("File does not exist: " + path);
                                        case HTTP_FORBIDDEN:
                                        case HTTP_BAD_REQUEST:
                                            throw new UnrecoverableS3OperationException(path, e);
                                    }
                                }
                                throw e;
                            }
                        });
            }
            catch (Exception e) {
                throw propagate(e);
            }
        }

        private void closeStream()
        {
            if (in != null) {
                abortStream(in);
                in = null;
                STATS.connectionReleased();
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
                if (in instanceof ResponseInputStream<?>) {
                    ((ResponseInputStream<?>) in).abort();
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

    private static class PrestoS3OutputStream
            extends FilterOutputStream
    {
        private final S3TransferManager transferManager;
        private final S3AsyncClient s3AsyncClient;
        private final String host;
        private final String key;
        private final File tempFile;
        private final boolean sseEnabled;
        private final PrestoS3SseType sseType;
        private final String sseKmsKeyId;
        private final ObjectCannedACL aclType;
        private final StorageClass s3StorageClass;
        private boolean closed;
        private final boolean disableChecksums;

        public PrestoS3OutputStream(
                S3Client s3,
                AwsCredentialsProvider credentialsProvider,
                Region region,
                String endpoint,
                boolean isPathStyleAccess,
                String host,
                String key,
                File tempFile,
                boolean sseEnabled,
                PrestoS3SseType sseType,
                String sseKmsKeyId,
                long multiPartUploadMinFileSize,
                long multiPartUploadMinPartSize,
                PrestoS3AclType aclType,
                PrestoS3StorageClass s3StorageClass,
                boolean disableChecksums)
                throws IOException
        {
            super(new BufferedOutputStream(Files.newOutputStream(requireNonNull(tempFile, "tempFile is null").toPath())));

            requireNonNull(s3, "s3 is null");
            this.disableChecksums = disableChecksums;
            // S3AsyncClient create
            S3AsyncClientBuilder asyncBuilder = S3AsyncClient.builder()
                    .credentialsProvider(requireNonNull(credentialsProvider, "credentialsProvider is null"))
                    .region(requireNonNull(region, "region is null"))
                    .serviceConfiguration(S3Configuration.builder()
                            .checksumValidationEnabled(!disableChecksums)
                            .build());

            if (endpoint != null) {
                asyncBuilder.endpointOverride(URI.create(endpoint));
            }

            if (isPathStyleAccess) {
                asyncBuilder.forcePathStyle(true);
            }

            this.s3AsyncClient = asyncBuilder.build();

            // Transfer Manager with custom configuration , passing s3AsyncClient
            this.transferManager = S3TransferManager.builder()
                    .s3Client(s3AsyncClient)
                    .build();

            this.host = requireNonNull(host, "host is null");
            this.key = requireNonNull(key, "key is null");
            this.tempFile = tempFile;
            this.sseEnabled = sseEnabled;
            this.sseType = requireNonNull(sseType, "sseType is null");
            this.sseKmsKeyId = sseKmsKeyId;
            this.aclType = convertToObjectCannedACL(requireNonNull(aclType, "aclType is null"));
            this.s3StorageClass = convertToStorageClass(requireNonNull(s3StorageClass, "s3StorageClass is null"));

            log.debug("OutputStream for key '%s' using file: %s", key, tempFile);
        }

        private static ObjectCannedACL convertToObjectCannedACL(PrestoS3AclType aclType)
        {
            switch (aclType) {
                case PRIVATE:
                    return ObjectCannedACL.PRIVATE;
                case PUBLIC_READ:
                    return ObjectCannedACL.PUBLIC_READ;
                case PUBLIC_READ_WRITE:
                    return ObjectCannedACL.PUBLIC_READ_WRITE;
                case AUTHENTICATED_READ:
                    return ObjectCannedACL.AUTHENTICATED_READ;
                case BUCKET_OWNER_FULL_CONTROL:
                    return ObjectCannedACL.BUCKET_OWNER_FULL_CONTROL;
                case BUCKET_OWNER_READ:
                    return ObjectCannedACL.BUCKET_OWNER_READ;
                default:
                    return ObjectCannedACL.PRIVATE;
            }
        }

        private static StorageClass convertToStorageClass(PrestoS3StorageClass storageClass)
        {
            switch (storageClass) {
                case STANDARD:
                    return StorageClass.STANDARD;
                case INTELLIGENT_TIERING:
                    return StorageClass.INTELLIGENT_TIERING;
                default:
                    return StorageClass.STANDARD;
            }
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
                try {
                    if (!tempFile.delete()) {
                        log.warn("Could not delete temporary file: %s", tempFile);
                    }
                }
                finally {
                    try {
                        transferManager.close();
                    }
                    finally {
                        s3AsyncClient.close();
                    }
                }
            }
        }

        private void uploadObject()
                throws IOException
        {
            try {
                log.debug("Starting upload for host: %s, key: %s, file: %s, size: %s", host, key, tempFile, tempFile.length());
                STATS.uploadStarted();

                PutObjectRequest.Builder requestBuilder = PutObjectRequest.builder()
                        .bucket(host)
                        .key(key)
                        .storageClass(s3StorageClass)
                        .acl(aclType);

                // Handle server-side encryption
                if (sseEnabled) {
                    switch (sseType) {
                        case KMS:
                            requestBuilder.serverSideEncryption(ServerSideEncryption.AWS_KMS);
                            if (sseKmsKeyId != null) {
                                requestBuilder.ssekmsKeyId(sseKmsKeyId);
                            }
                            break;
                        case S3:
                            requestBuilder.serverSideEncryption(ServerSideEncryption.AES256);
                            break;
                    }
                }

                // Create UploadFileRequest with optional progress listener
                UploadFileRequest uploadFileRequest = UploadFileRequest.builder()
                        .putObjectRequest(requestBuilder.build())
                        .source(tempFile.toPath())
                        .addTransferListener(createTransferListener())
                        .build();

                // upload using Transfer Manager
                FileUpload fileUpload = transferManager.uploadFile(uploadFileRequest);

                // Wait for completion (blocking call)
                CompletedFileUpload completedUpload = fileUpload.completionFuture().join();

                STATS.uploadSuccessful();
                log.debug("Completed upload for host: %s, key: %s, ETag: %s",
                        host, key, completedUpload.response().eTag());
            }
            catch (Exception e) {
                STATS.uploadFailed();
                if (e.getCause() instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                    throw new InterruptedIOException();
                }
                throw new IOException("Upload failed for key: " + key, e);
            }
        }

        private TransferListener createTransferListener()
        {
            return new TransferListener() {
                private long previousBytesTransferred;

                @Override
                public void transferInitiated(Context.TransferInitiated context)
                {
                    log.debug("Upload initiated for %s/%s", host, key);
                }

                @Override
                public void bytesTransferred(Context.BytesTransferred context)
                {
                    long currentBytes = context.progressSnapshot().transferredBytes();
                    if (currentBytes - previousBytesTransferred >= 10 * 1024 * 1024) {
                        log.debug("Upload progress (%s/%s): %d bytes transferred",
                                host, key, currentBytes);
                        previousBytesTransferred = currentBytes;
                    }
                }

                @Override
                public void transferComplete(Context.TransferComplete context)
                {
                    log.debug("Upload completed for %s/%s", host, key);
                }

                @Override
                public void transferFailed(Context.TransferFailed context)
                {
                    log.error("Upload failed for %s/%s: %s",
                            host, key, context.exception().getMessage());
                }
            };
        }
    }

    @VisibleForTesting
    S3Client getS3Client()
    {
        return s3;
    }

    @VisibleForTesting
    void setS3Client(S3Client client)
    {
        s3 = client;
    }

    /**
     * Helper function used to work around the fact that if you use an S3 bucket with an '_' that java.net.URI
     * behaves differently and sets the host value to null whereas S3 buckets without '_' have a properly
     * set host field. '_' is only allowed in S3 bucket names in us-east-1.
     *
     * @param uri The URI from which to extract a host value.
     * @return The host value where uri.getAuthority() is used when uri.getHost() returns null as long as no UserInfo is present.
     * @throws IllegalArgumentException If the bucket can not be determined from the URI.
     */
    public static String getBucketName(URI uri)
    {
        if (uri.getHost() != null) {
            return uri.getHost();
        }

        if (uri.getUserInfo() == null) {
            return uri.getAuthority();
        }

        throw new IllegalArgumentException("Unable to determine S3 bucket from URI.");
    }

    public static PrestoS3FileSystemStats getFileSystemStats()
    {
        return STATS;
    }

    public static PrestoS3FileSystemMetricCollector getMetricsCollector()
    {
        return metricCollector;
    }

    public static void setMetricsCollector(PrestoS3FileSystemMetricCollector customMetricCollector)
    {
        metricCollector = customMetricCollector;
    }
}
