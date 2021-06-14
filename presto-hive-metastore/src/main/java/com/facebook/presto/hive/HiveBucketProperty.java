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

import com.facebook.presto.common.BucketingUtils;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.metastore.SortingColumn;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.hive.BucketFunctionType.HIVE_CLUSTERING;
import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.BucketFunctionType.PRESTO_NATIVE;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_TABLE_PROPERTY;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class HiveBucketProperty
{
    private final List<String> bucketedBy;
    private final int bucketCount;
    private final List<SortingColumn> sortedBy;
    private final BucketFunctionType bucketFunctionType;
    private final Optional<List<Type>> types;
    private final Optional<List<String>> clusteredBy;
    private final Optional<List<Integer>> clusterCount;
    private final Optional<List<Object>> distribution;

    @JsonCreator
    public HiveBucketProperty(
            @JsonProperty("bucketedBy") List<String> bucketedBy,
            @JsonProperty("bucketCount") int bucketCount,
            @JsonProperty("sortedBy") List<SortingColumn> sortedBy,
            @JsonProperty("bucketFunctionType") BucketFunctionType bucketFunctionType,
            @JsonProperty("types") Optional<List<Type>> types,
            @JsonProperty("clusteredBy") Optional<List<String>> clusteredBy,
            @JsonProperty("clusterCount") Optional<List<Integer>> clusterCount,
            @JsonProperty("distribution") Optional<List<Object>> distribution)
    {
        this.bucketedBy = ImmutableList.copyOf(requireNonNull(bucketedBy, "bucketedBy is null"));
        this.bucketCount = bucketCount;
        this.sortedBy = ImmutableList.copyOf(requireNonNull(sortedBy, "sortedBy is null"));
        this.bucketFunctionType = requireNonNull(bucketFunctionType, "bucketFunctionType is null");
        this.types = requireNonNull(types, "type is null");
        this.clusteredBy = requireNonNull(clusteredBy, "clusteredBy is null");
        this.clusterCount = requireNonNull(clusterCount, "clusterCount is null");
        this.distribution = requireNonNull(distribution, "distribution is null");

        if (bucketFunctionType.equals(PRESTO_NATIVE)) {
            checkArgument(types.isPresent(), "Types must be present for bucket function type " + bucketFunctionType);
            checkArgument(types.get().size() == bucketedBy.size(), "The sizes of bucketedBy and types should match");
        }
        else if (bucketFunctionType.equals(HIVE_CLUSTERING)) {
            checkArgument(clusteredBy.isPresent(), "Cluster columns must be present for bucket function type " + bucketFunctionType);
            checkArgument(clusterCount.isPresent(), "Cluster numbers must be present for bucket function type " + bucketFunctionType);
            checkArgument(bucketedBy.isEmpty(), "Bucket columns should not be present for bucket function type " + bucketFunctionType);
        }
        else {
            checkArgument(!types.isPresent(), "Types not needed for bucket function type " + bucketFunctionType);
        }
    }

    public static Optional<HiveBucketProperty> fromStorageDescriptor(StorageDescriptor storageDescriptor, String tablePartitionName)
    {
        boolean bucketColsSet = storageDescriptor.isSetBucketCols() && !storageDescriptor.getBucketCols().isEmpty();
        boolean numBucketsSet = storageDescriptor.isSetNumBuckets() && storageDescriptor.getNumBuckets() > 0;
        if (!numBucketsSet) {
            // In Hive, a table is considered as not bucketed when its bucketCols is set but its numBucket is not set.
            return Optional.empty();
        }

        Map<String, String> storageDescriptorParameters = storageDescriptor.getParameters();
        List<String> clusteredBy = BucketingUtils.getClusteredByFromEncoding(storageDescriptorParameters);
        boolean doClustering = BucketingUtils.doClustering(clusteredBy);
//        if (!bucketColsSet && !doClustering) {
//            throw new PrestoException(
//                    HIVE_INVALID_METADATA,
//                    "Table/partition metadata has 'numBuckets' set, but 'bucketCols' is not set: " + tablePartitionName);
//        }

        List<SortingColumn> sortedBy = ImmutableList.of();
        if (storageDescriptor.isSetSortCols()) {
            sortedBy = storageDescriptor.getSortCols().stream()
                    .map(order -> SortingColumn.fromMetastoreApiOrder(order, tablePartitionName))
                    .collect(toImmutableList());
        }

        if (doClustering) {
            List<Integer> clusterCount = BucketingUtils.getClusterCountFromEncoding(storageDescriptorParameters);
            List<Object> distribution = BucketingUtils.getDistributionFromEncoding(storageDescriptorParameters);

            if (clusterCount.isEmpty() || distribution.isEmpty()) {
                throw new PrestoException(INVALID_TABLE_PROPERTY, "To do clustering, cluster_count and distribution must be specified");
            }

            return Optional.of(new HiveBucketProperty(
                    Collections.emptyList(),
                    storageDescriptor.getNumBuckets(),
                    sortedBy,
                    HIVE_CLUSTERING,
                    Optional.empty(),
                    Optional.of(clusteredBy),
                    Optional.of(clusterCount),
                    Optional.of(distribution)));
        }

        return Optional.of(new HiveBucketProperty(
                storageDescriptor.getBucketCols(),
                storageDescriptor.getNumBuckets(),
                sortedBy,
                HIVE_COMPATIBLE,
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty()));
    }

    @JsonProperty
    public List<String> getBucketedBy()
    {
        return bucketedBy;
    }

    @JsonProperty
    public int getBucketCount()
    {
        if (bucketFunctionType != HIVE_CLUSTERING) {
            return bucketCount;
        }

        return getMergedClusterCount(clusterCount.get());
    }

    private static int getMergedClusterCount(List<Integer> clusterCount)
    {
        int combinedClusterCount = 1;
        for (int count : clusterCount) {
            combinedClusterCount *= count;
        }
        return combinedClusterCount;
    }

    @JsonProperty
    public List<SortingColumn> getSortedBy()
    {
        return sortedBy;
    }

    @JsonProperty
    public BucketFunctionType getBucketFunctionType()
    {
        return bucketFunctionType;
    }

    @JsonProperty
    public Optional<List<Type>> getTypes()
    {
        return types;
    }

    @JsonProperty
    public Optional<List<String>> getClusteredBy()
    {
        return clusteredBy;
    }

    @JsonProperty
    public Optional<List<Integer>> getClusterCount()
    {
        return clusterCount;
    }

    @JsonProperty
    public Optional<List<Object>> getDistribution()
    {
        return distribution;
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
                Objects.equals(sortedBy, that.sortedBy) &&
                bucketFunctionType.equals(that.bucketFunctionType) &&
                Objects.equals(types, that.types);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketedBy, bucketCount, sortedBy, bucketFunctionType, types);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketedBy", bucketedBy)
                .add("bucketCount", bucketCount)
                .add("sortedBy", sortedBy)
                .add("types", types)
                .add("bucketFunctionType", bucketFunctionType)
                .toString();
    }
}
