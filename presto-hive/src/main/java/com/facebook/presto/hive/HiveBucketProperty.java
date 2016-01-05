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

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class HiveBucketProperty
{
    private final List<String> clusteredBy;
    private final int bucketCount;

    @JsonCreator
    public HiveBucketProperty(@JsonProperty("clusteredBy") List<String> clusteredBy, @JsonProperty("bucketCount") int bucketCount)
    {
        this.clusteredBy = requireNonNull(clusteredBy, "clusteredBy is null");
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
    }

    public static Optional<HiveBucketProperty> fromStorageDescriptor(StorageDescriptor storageDescriptor, String tablePartitionName)
    {
        boolean bucketColsSet = storageDescriptor.isSetBucketCols() && !storageDescriptor.getBucketCols().isEmpty();
        boolean numBucketsSet = storageDescriptor.isSetNumBuckets() && storageDescriptor.getNumBuckets() > 0;
        if (bucketColsSet != numBucketsSet) {
            throw new PrestoException(HiveErrorCode.HIVE_INVALID_METADATA, "Only one of bucketCols and numBuckets is set in metadata of table/partition " + tablePartitionName);
        }
        if (!bucketColsSet) {
            return Optional.empty();
        }
        return Optional.of(new HiveBucketProperty(storageDescriptor.getBucketCols(), storageDescriptor.getNumBuckets()));
    }

    @JsonProperty
    public List<String> getClusteredBy()
    {
        return clusteredBy;
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
                Objects.equals(clusteredBy, that.clusteredBy);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(clusteredBy, bucketCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("clusteredBy", clusteredBy)
                .add("bucketCount", bucketCount)
                .toString();
    }
}
