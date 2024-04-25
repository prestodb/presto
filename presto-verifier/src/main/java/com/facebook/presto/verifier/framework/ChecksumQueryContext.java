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
package com.facebook.presto.verifier.framework;

import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class ChecksumQueryContext
{
    private Optional<String> checksumQueryId = Optional.empty();
    private Optional<String> checksumQuery = Optional.empty();

    private Optional<String> partitionChecksumQueryId = Optional.empty();
    private Optional<String> partitionChecksumQuery = Optional.empty();

    private Optional<String> bucketChecksumQueryId = Optional.empty();
    private Optional<String> bucketChecksumQuery = Optional.empty();

    public Optional<String> getChecksumQueryId()
    {
        return checksumQueryId;
    }

    public void setChecksumQueryId(String checksumQueryId)
    {
        checkState(!this.checksumQueryId.isPresent(), "controlChecksumQueryId is already set");
        this.checksumQueryId = Optional.of(checksumQueryId);
    }

    public Optional<String> getChecksumQuery()
    {
        return checksumQuery;
    }

    public void setChecksumQuery(String checksumQuery)
    {
        checkState(!this.checksumQuery.isPresent(), "controlChecksumQuery is already set");
        this.checksumQuery = Optional.of(checksumQuery);
    }

    public Optional<String> getPartitionChecksumQueryId()
    {
        return partitionChecksumQueryId;
    }

    public void setPartitionChecksumQueryId(String partitionChecksumQueryId)
    {
        checkState(!this.partitionChecksumQueryId.isPresent(), "partitionChecksumQueryId is already set");
        this.partitionChecksumQueryId = Optional.of(partitionChecksumQueryId);
    }

    public Optional<String> getPartitionChecksumQuery()
    {
        return partitionChecksumQuery;
    }

    public void setPartitionChecksumQuery(String partitionChecksumQuery)
    {
        checkState(!this.partitionChecksumQuery.isPresent(), "partitionChecksumQuery is already set");
        this.partitionChecksumQuery = Optional.of(partitionChecksumQuery);
    }

    public Optional<String> getBucketChecksumQueryId()
    {
        return bucketChecksumQueryId;
    }

    public void setBucketChecksumQueryId(String bucketChecksumQueryId)
    {
        checkState(!this.bucketChecksumQueryId.isPresent(), "bucketChecksumQueryId is already set");
        this.bucketChecksumQueryId = Optional.of(bucketChecksumQueryId);
    }

    public Optional<String> getBucketChecksumQuery()
    {
        return bucketChecksumQuery;
    }

    public void setBucketChecksumQuery(String bucketChecksumQuery)
    {
        checkState(!this.bucketChecksumQuery.isPresent(), "bucketChecksumQuery is already set");
        this.bucketChecksumQuery = Optional.of(bucketChecksumQuery);
    }
}
