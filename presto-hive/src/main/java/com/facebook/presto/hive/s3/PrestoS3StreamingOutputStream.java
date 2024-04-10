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

import com.amazonaws.AmazonClientException;
import com.amazonaws.event.ProgressEvent;
import com.amazonaws.event.ProgressEventType;
import com.amazonaws.event.ProgressListener;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.AbortMultipartUploadRequest;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.CompleteMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadRequest;
import com.amazonaws.services.s3.model.InitiateMultipartUploadResult;
import com.amazonaws.services.s3.model.ListMultipartUploadsRequest;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.model.UploadPartRequest;
import com.amazonaws.services.s3.model.UploadPartResult;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.amazonaws.services.s3.transfer.model.UploadResult;
import com.facebook.airlift.log.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import static com.facebook.presto.hive.RetryDriver.retry;
import static java.nio.file.Files.createTempFile;
import static java.util.Objects.requireNonNull;

public class PrestoS3StreamingOutputStream
        extends FilterOutputStream
{
    private static final Logger log = Logger.get(PrestoS3StreamingOutputStream.class);

    private final AmazonS3 client;
    private final String host;
    private final String key;
    private final File tempDir;
    private final boolean sseEnabled;
    private final PrestoS3SseType sseType;
    private final String sseKmsKeyId;
    private final CannedAccessControlList aclType;
    private final StorageClass s3StorageClass;

    private java.nio.file.Path partFile;
    private short currentPart;
    private final List<UploadPartResult> partList = new ArrayList<>();
    long currentPartWrittenBytes;
    private final long minPartUploadFileSize;
    private final long minPartSize;
    private final PrestoS3FileSystemStats stats;

    Optional<InitiateMultipartUploadResult> upload = Optional.empty();

    private boolean closed;

    public PrestoS3StreamingOutputStream(
            AmazonS3 s3,
            String host,
            String key,
            File tempDir,
            boolean sseEnabled,
            PrestoS3SseType sseType,
            String sseKmsKeyId,
            long multiPartUploadMinFileSize,
            long multiPartUploadMinPartSize,
            PrestoS3AclType aclType,
            PrestoS3StorageClass s3StorageClass,
            PrestoS3FileSystemStats stats)
            throws IOException
    {
        super(null); // null so we can initialize it with state later
        minPartSize = multiPartUploadMinPartSize;
        minPartUploadFileSize = multiPartUploadMinFileSize;
        client = requireNonNull(s3, "s3 is null");
        requireNonNull(aclType, "aclType is null");
        requireNonNull(s3StorageClass, "s3StorageClass is null");
        this.aclType = aclType.getCannedACL();
        this.host = requireNonNull(host, "host is null");
        this.key = requireNonNull(key, "key is null");
        this.tempDir = requireNonNull(tempDir, "tempDir is null");
        this.sseEnabled = sseEnabled;
        this.sseType = requireNonNull(sseType, "sseType is null");
        this.sseKmsKeyId = sseKmsKeyId;
        this.s3StorageClass = s3StorageClass.getS3StorageClass();
        this.stats = requireNonNull(stats, "stats is null");

        // this call initializes the OutputStream
        newTempFileBuffer();
        log.debug("OutputStream for key '%s' using directory: %s", key, this.tempDir);
    }

    private void newTempFileBuffer()
            throws IOException
    {
        partFile = requireNonNull(createTempFile(tempDir.toPath(), "part-" + currentPart, ".tmp"), "tempFile is null");
        currentPart++;
        currentPartWrittenBytes = 0;
        out = new BufferedOutputStream(Files.newOutputStream(partFile));
    }

    @Override
    public void write(int b)
            throws IOException
    {
        if (currentPartWrittenBytes >= minPartSize) {
            flushPartUpload(false);
        }
        out.write(b);
        currentPartWrittenBytes += 1;
    }

    @Override
    public void write(byte[] b)
            throws IOException
    {
        if (currentPartWrittenBytes >= minPartSize) {
            flushPartUpload(false);
        }
        out.write(b);
        currentPartWrittenBytes += b.length;
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        if (currentPartWrittenBytes >= minPartSize) {
            flushPartUpload(false);
        }
        out.write(b, off, len);
        currentPartWrittenBytes += len;
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
            // if the upload wasn't initiated already, use the AWS transfer manager
            // otherwise, flush the final part and upload complete the multipart request
            String etag = upload.map(upload -> {
                flushPartUpload(true);
                return client.completeMultipartUpload(new CompleteMultipartUploadRequest()
                        .withBucketName(host)
                        .withKey(key)
                        .withPartETags(partList)
                        .withUploadId(upload.getUploadId())).getETag();
            }).orElseGet(this::uploadSingleFile);
            log.debug("Uploaded file to %s with ETag: %s", key, etag);
        }
        finally {
            try (Stream<Path> dirStream = Files.walk(tempDir.toPath())) {
                if (!dirStream.map(java.nio.file.Path::toFile)
                        // deletes all files first, then the directory
                        .sorted(Comparator.reverseOrder())
                        .map(File::delete)
                        .reduce(true, (a, b) -> a && b)) {
                    log.warn("Failed to delete all files in " + tempDir);
                }
            }
        }
    }

    private void flushPartUpload(boolean last)
    {
        try {
            if (!upload.isPresent()) {
                InitiateMultipartUploadRequest request = new InitiateMultipartUploadRequest(host, key)
                        .withStorageClass(s3StorageClass)
                        .withCannedACL(aclType);
                if (sseEnabled) {
                    switch (sseType) {
                        case KMS:
                            if (sseKmsKeyId != null) {
                                request.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(sseKmsKeyId));
                            }
                            else {
                                request.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams());
                            }
                            break;
                        case S3:
                            ObjectMetadata metadata = new ObjectMetadata();
                            metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                            request.withObjectMetadata(metadata);
                            break;
                    }
                }
                upload = Optional.of(client.initiateMultipartUpload(request));
            }
            InitiateMultipartUploadResult currentUpload = upload.get();
            out.close();
            stats.uploadStarted();
            UploadPartRequest request = new UploadPartRequest()
                    .withBucketName(host)
                    .withKey(key)
                    .withFile(partFile.toFile())
                    .withPartNumber(currentPart)
                    .withFileOffset(0)
                    .withLastPart(last)
                    .withPartSize(partFile.toFile().length())
                    .withUploadId(currentUpload.getUploadId());
            if (log.isDebugEnabled()) {
                request.withGeneralProgressListener(createProgressListener(partFile.toFile().length()));
            }

            partList.add(client.uploadPart(request));
            stats.uploadSuccessful();
            Files.deleteIfExists(partFile);
            if (!last) {
                newTempFileBuffer();
            }
        }
        catch (Exception e) {
            stats.uploadFailed();
            upload.ifPresent(this::cleanUpFailedMultipartUpload);
            throw new RuntimeException(e);
        }
    }

    private String uploadSingleFile()
    {
        TransferManager transfer = TransferManagerBuilder.standard()
                .withS3Client(client)
                .withMinimumUploadPartSize(minPartSize)
                .withMultipartUploadThreshold(minPartUploadFileSize)
                .build();
        try {
            stats.uploadStarted();
            PutObjectRequest request = new PutObjectRequest(host, key, partFile.toFile());
            if (sseEnabled) {
                switch (sseType) {
                    case KMS:
                        if (sseKmsKeyId != null) {
                            request.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams(sseKmsKeyId));
                        }
                        else {
                            request.withSSEAwsKeyManagementParams(new SSEAwsKeyManagementParams());
                        }
                        break;
                    case S3:
                        ObjectMetadata metadata = new ObjectMetadata();
                        metadata.setSSEAlgorithm(ObjectMetadata.AES_256_SERVER_SIDE_ENCRYPTION);
                        request.withMetadata(metadata);
                        break;
                }
            }
            request.withStorageClass(s3StorageClass);
            request.withCannedAcl(aclType);
            Upload upload = transfer.upload(request);
            if (log.isDebugEnabled()) {
                upload.addProgressListener(createProgressListener(partFile.toFile().length()));
            }
            UploadResult result = upload.waitForUploadResult();
            stats.uploadSuccessful();
            return result.getETag();
        }
        catch (AmazonClientException e) {
            stats.uploadFailed();
            throw new RuntimeException(e);
        }
        catch (InterruptedException e) {
            stats.uploadFailed();
            Thread.currentThread().interrupt();
            throw new RuntimeException(e);
        }
        finally {
            transfer.shutdownNow(false);
        }
    }

    /**
     * In the case of errors, make a best-effort to clean up the failed multipart upload. Improperly
     * aborted uploads can lead to additional S3 costs for "ghost" files. In order to ensure that
     * everything is properly cleaned up, the ListParts request must return empty.
     * <br>
     * See <a href="https://docs.aws.amazon.com/AmazonS3/latest/API/API_AbortMultipartUpload.html"></a>
     *
     * @param currentUpload the original multipart upload request
     */
    private void cleanUpFailedMultipartUpload(InitiateMultipartUploadResult currentUpload)
    {
        try {
            while (retry()
                    // theoretically, listMultipartUploads can return > 1k results, requiring paging to search for the existing upload.
                    // However, since we filter using the `withPrefix` as the file name, we assume that there won't be > 1000 results.
                    .run("s3-listMultipartUploads", () -> client.listMultipartUploads(new ListMultipartUploadsRequest(host).withPrefix(key))
                            .getMultipartUploads().stream().anyMatch(upload -> upload.getUploadId().equals(currentUpload.getUploadId())))) {
                retry().run("s3-AbortMultipartUpload", () -> {
                    client.abortMultipartUpload(new AbortMultipartUploadRequest(host, key, currentUpload.getUploadId()));
                    return null;
                });
            }
        }
        catch (Exception e) {
            String errMessage = String.format("Failed to abort multipart upload at s3://%s/%s with uploadId: %s. User should manually abort upload to prevent additional S3 costs.", currentUpload.getBucketName(), currentUpload.getKey(), currentUpload.getUploadId());
            log.error(errMessage);
            throw new RuntimeException(errMessage, e);
        }
    }

    private ProgressListener createProgressListener(long totalSize)
    {
        return new ProgressListener()
        {
            private final long maxSize = totalSize;
            private long totalTransferred;
            private ProgressEventType previousType;
            private double previousTransferred;

            @Override
            public synchronized void progressChanged(ProgressEvent progressEvent)
            {
                ProgressEventType eventType = progressEvent.getEventType();
                totalTransferred += progressEvent.getBytesTransferred();
                if (previousType != eventType) {
                    log.debug("Upload progress event (%s/%s): %s", host, key, eventType);
                    previousType = eventType;
                }

                double transferred = (double) totalTransferred / maxSize;
                if (transferred >= (previousTransferred + 10.0)) {
                    log.debug("Upload percentage (%s/%s): %.0f%%", host, key, transferred);
                    previousTransferred = transferred;
                }
            }
        };
    }
}
