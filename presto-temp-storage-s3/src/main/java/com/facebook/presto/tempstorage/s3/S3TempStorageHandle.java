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

import com.facebook.presto.spi.storage.TempStorageHandle;

import static java.util.Objects.requireNonNull;

public class S3TempStorageHandle
        implements TempStorageHandle
{
    private final String bucket;
    private final String key;

    public S3TempStorageHandle(String bucket, String key)
    {
        this.bucket = requireNonNull(bucket, "bucket is null");
        this.key = requireNonNull(key, "key is null");
    }

    public String getBucket()
    {
        return bucket;
    }

    public String getKey()
    {
        return key;
    }

    @Override
    public String getPathAsString()
    {
        return "s3://" + bucket + "/" + key;
    }

    @Override
    public String toString()
    {
        return getPathAsString();
    }
}
