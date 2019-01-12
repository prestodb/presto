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
package io.prestosql.plugin.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.prestosql.plugin.hive.metastore.SortingColumn;
import io.prestosql.spi.PrestoException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.prestosql.plugin.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static java.util.Objects.requireNonNull;

public class HiveBucketProperty
{
    private final List<String> bucketedBy;
    private final int bucketCount;
    private final List<SortingColumn> sortedBy;

    @JsonCreator
    public HiveBucketProperty(
            @JsonProperty("bucketedBy") List<String> bucketedBy,
            @JsonProperty("bucketCount") int bucketCount,
            @JsonProperty("sortedBy") List<SortingColumn> sortedBy)
    {
        this.bucketedBy = ImmutableList.copyOf(requireNonNull(bucketedBy, "bucketedBy is null"));
        this.bucketCount = bucketCount;
        this.sortedBy = ImmutableList.copyOf(requireNonNull(sortedBy, "sortedBy is null"));
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
        List<SortingColumn> sortedBy = ImmutableList.of();
        if (storageDescriptor.isSetSortCols()) {
            sortedBy = storageDescriptor.getSortCols().stream()
                    .map(order -> SortingColumn.fromMetastoreApiOrder(order, tablePartitionName))
                    .collect(toImmutableList());
        }
        return Optional.of(new HiveBucketProperty(storageDescriptor.getBucketCols(), storageDescriptor.getNumBuckets(), sortedBy));
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

    @JsonProperty
    public List<SortingColumn> getSortedBy()
    {
        return sortedBy;
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
                Objects.equals(bucketedBy, that.bucketedBy) &&
                Objects.equals(sortedBy, that.sortedBy);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketedBy, bucketCount, sortedBy);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketedBy", bucketedBy)
                .add("bucketCount", bucketCount)
                .add("sortedBy", sortedBy)
                .toString();
    }
}
