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
package com.facebook.presto.tempstorage.s3;

import com.facebook.presto.common.io.DataOutput;
import com.facebook.presto.spi.storage.TempDataSink;
import com.facebook.presto.spi.storage.TempStorageHandle;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.SliceOutput;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import java.io.IOException;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class S3TempDataSink
        implements TempDataSink
{
    private static final int INITIAL_OUTPUT_SIZE = 1024;

    private final S3Client s3Client;
    private final String bucket;
    private final String key;
    private final SliceOutput output = new DynamicSliceOutput(INITIAL_OUTPUT_SIZE);

    public S3TempDataSink(S3Client s3Client, String bucket, String key)
    {
        this.s3Client = requireNonNull(s3Client, "s3Client is null");
        this.bucket = requireNonNull(bucket, "bucket is null");
        this.key = requireNonNull(key, "key is null");
    }

    @Override
    public TempStorageHandle commit()
            throws IOException
    {
        try {
            PutObjectRequest.Builder request = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(key);

            s3Client.putObject(request.build(), RequestBody.fromBytes(output.slice().getBytes()));
            return new S3TempStorageHandle(bucket, key);
        }
        catch (S3Exception e) {
            throw new IOException("Failed to upload object to S3", e);
        }
    }

    @Override
    public void rollback()
    {
        output.reset();
    }

    @Override
    public long size()
    {
        return output.size();
    }

    @Override
    public long getRetainedSizeInBytes()
    {
        return output.getRetainedSize();
    }

    @Override
    public void write(List<DataOutput> outputData)
            throws IOException
    {
        for (DataOutput data : outputData) {
            data.writeData(output);
        }
    }

    @Override
    public void close()
    {
    }
}
