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
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.SSEAwsKeyManagementParams;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.Transfer;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import com.facebook.airlift.log.Logger;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.nio.file.Files;

import static java.util.Objects.requireNonNull;

public class PrestoS3OutputStream
        extends FilterOutputStream
{
    private static final Logger log = Logger.get(PrestoS3OutputStream.class);
    private final TransferManager transferManager;
    private final String host;
    private final String key;
    private final File tempFile;
    private final boolean sseEnabled;
    private final PrestoS3SseType sseType;
    private final String sseKmsKeyId;
    private final CannedAccessControlList aclType;
    private final StorageClass s3StorageClass;
    private final PrestoS3FileSystemStats stats;

    private boolean closed;

    public PrestoS3OutputStream(
            AmazonS3 s3,
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
            PrestoS3FileSystemStats stats)
            throws IOException
    {
        super(new BufferedOutputStream(Files.newOutputStream(requireNonNull(tempFile, "tempFile is null").toPath())));

        transferManager = TransferManagerBuilder.standard()
                .withS3Client(requireNonNull(s3, "s3 is null"))
                .withMinimumUploadPartSize(multiPartUploadMinPartSize)
                .withMultipartUploadThreshold(multiPartUploadMinFileSize).build();

        requireNonNull(aclType, "aclType is null");
        requireNonNull(s3StorageClass, "s3StorageClass is null");
        this.aclType = aclType.getCannedACL();
        this.host = requireNonNull(host, "host is null");
        this.key = requireNonNull(key, "key is null");
        this.tempFile = tempFile;
        this.sseEnabled = sseEnabled;
        this.sseType = requireNonNull(sseType, "sseType is null");
        this.sseKmsKeyId = sseKmsKeyId;
        this.s3StorageClass = s3StorageClass.getS3StorageClass();
        this.stats = requireNonNull(stats, "stats is null");

        log.debug("OutputStream for key '%s' using file: %s", key, tempFile);
    }

    @Override
    public void write(byte[] b)
            throws IOException
    {
        out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len)
            throws IOException
    {
        out.write(b, off, len);
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
            stats.uploadStarted();

            PutObjectRequest request = new PutObjectRequest(host, key, tempFile);
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
                        request.setMetadata(metadata);
                        break;
                }
            }
            request.withStorageClass(s3StorageClass);

            request.withCannedAcl(aclType);

            Upload upload = transferManager.upload(request);

            if (log.isDebugEnabled()) {
                upload.addProgressListener(createProgressListener(upload));
            }

            upload.waitForCompletion();
            stats.uploadSuccessful();
            log.debug("Completed upload for host: %s, key: %s", host, key);
        }
        catch (AmazonClientException e) {
            stats.uploadFailed();
            throw new IOException(e);
        }
        catch (InterruptedException e) {
            stats.uploadFailed();
            Thread.currentThread().interrupt();
            throw new InterruptedIOException();
        }
    }

    private ProgressListener createProgressListener(Transfer transfer)
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
