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

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HiveBucketProperty
{
    private final List<String> bucketedBy;
    private final int bucketCount;

    @JsonCreator
    public HiveBucketProperty(
            @JsonProperty("bucketedBy") List<String> bucketedBy,
            @JsonProperty("bucketCount") int bucketCount)
    {
        this.bucketedBy = requireNonNull(bucketedBy, "bucketedBy is null");
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
    }

    public static Optional<HiveBucketProperty> fromStorageDescriptor(StorageDescriptor storageDescriptor, String tablePartitionName)
    {
        boolean bucketColsSet = storageDescriptor.isSetBucketCols() && !storageDescriptor.getBucketCols().isEmpty();
        boolean numBucketsSet = storageDescriptor.isSetNumBuckets() && storageDescriptor.getNumBuckets() > 0;
        if (!numBucketsSet) {
            // In Hive, a table is considered as not bucketed when its bucketCols is set but its numBucket is not set.
            return Optional.empty();
        }
        if (!bucketColsSet) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table/partition metadata has 'numBuckets' set, but 'bucketCols' is not set: " + tablePartitionName);
        }
        return Optional.of(new HiveBucketProperty(storageDescriptor.getBucketCols(), storageDescriptor.getNumBuckets()));
    }

    @JsonProperty
    public List<String> getBucketedBy()
    {
        return bucketedBy;
    }

    @JsonProperty
    public int getBucketCount()
    {
        return bucketCount;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        HiveBucketProperty that = (HiveBucketProperty) o;
        return bucketCount == that.bucketCount &&
                Objects.equals(bucketedBy, that.bucketedBy);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketedBy, bucketCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketedBy", bucketedBy)
                .add("bucketCount", bucketCount)
                .toString();
    }
}
