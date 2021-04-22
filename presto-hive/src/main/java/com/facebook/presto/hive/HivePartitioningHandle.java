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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.connector.ConnectorPartitioningHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

import static com.facebook.presto.hive.BucketFunctionType.HIVE_CLUSTERING;
import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.BucketFunctionType.PRESTO_NATIVE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HivePartitioningHandle
        implements ConnectorPartitioningHandle
{
    private final int bucketCount;
    private final OptionalInt maxCompatibleBucketCount;
    private final BucketFunctionType bucketFunctionType;
    private final Optional<List<HiveType>> hiveTypes;
    private final Optional<List<Type>> types;

    private final Optional<List<String>> clusteredBy;
    private final Optional<List<Integer>> clusterCount;
    private final Optional<List<String>> distribution;

    public static HivePartitioningHandle createHiveCompatiblePartitioningHandle(
            int bucketCount,
            List<HiveType> hiveTypes,
            OptionalInt maxCompatibleBucketCount)
    {
        return new HivePartitioningHandle(
                bucketCount,
                maxCompatibleBucketCount,
                HIVE_COMPATIBLE,
                Optional.of(hiveTypes),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public static HivePartitioningHandle createPrestoNativePartitioningHandle(
            int bucketCount,
            List<Type> types,
            OptionalInt maxCompatibleBucketCount)
    {
        return new HivePartitioningHandle(
                bucketCount,
                maxCompatibleBucketCount,
                PRESTO_NATIVE,
                Optional.empty(),
                Optional.of(types),
                Optional.empty(),
                Optional.empty(),
                Optional.empty());
    }

    public static HivePartitioningHandle createClusteringPartitioningHandle(
            List<Type> types,
            OptionalInt maxCompatibleBucketCount,
            List<String> clusteredBy,
            List<Integer> clusterCount,
            List<String> distribution)
    {
        return new HivePartitioningHandle(
                0,
                maxCompatibleBucketCount,
                HIVE_CLUSTERING,
                Optional.empty(),
                Optional.of(types),
                Optional.of(clusteredBy),
                Optional.of(clusterCount),
                Optional.of(distribution));
    }

    @JsonCreator
    public HivePartitioningHandle(
            @JsonProperty("bucketCount") int bucketCount,
            @JsonProperty("maxCompatibleBucketCount") OptionalInt maxCompatibleBucketCount,
            @JsonProperty("bucketFunctionType") BucketFunctionType bucketFunctionType,
            @JsonProperty("hiveTypes") Optional<List<HiveType>> hiveTypes,
            @JsonProperty("types") Optional<List<Type>> types,
            @JsonProperty("clusteredBy") Optional<List<String>> clusteredBy,
            @JsonProperty("clusterCount") Optional<List<Integer>> clusterCount,
            @JsonProperty("distribution")Optional<List<String>> distribution)
    {
        this.bucketCount = bucketCount;
        this.maxCompatibleBucketCount = maxCompatibleBucketCount;
        this.bucketFunctionType = requireNonNull(bucketFunctionType, "bucketFunctionType is null");
        this.hiveTypes = requireNonNull(hiveTypes, "hiveTypes is null");
        this.types = requireNonNull(types, "types is null");

        this.clusteredBy = requireNonNull(clusteredBy, "Clustering columns are null");
        this.clusterCount = requireNonNull(clusterCount, "The number of clusters for each clustering column");
        this.distribution = requireNonNull(distribution, "distribution is null");

        checkArgument((
                bucketFunctionType.equals(HIVE_COMPATIBLE) && hiveTypes.isPresent() && !types.isPresent()) ||
                        ((bucketFunctionType.equals(PRESTO_NATIVE) || bucketFunctionType.equals(HIVE_CLUSTERING)) &&
                                !hiveTypes.isPresent() && types.isPresent()),
                "Type list for bucketFunctionType %s is missing or duplicated. hiveTypes: %s, types: %s", bucketFunctionType,
                hiveTypes,
                types);
    }

    @JsonProperty
    public int getBucketCount()
    {
        if (bucketFunctionType == HIVE_COMPATIBLE || bucketFunctionType == PRESTO_NATIVE) {
            return bucketCount;
        }

        return getMergedClusterCount(clusterCount.get());
    }

    // TODO: Consolidate the same logic in HiveBucktProperty.
    private static int getMergedClusterCount(List<Integer> clusterCount)
    {
        int mergedClusterCount = 1;
        for (int count : clusterCount) {
            mergedClusterCount *= count;
        }
        return mergedClusterCount;
    }

    @JsonProperty
    public Optional<List<HiveType>> getHiveTypes()
    {
        return hiveTypes;
    }

    @JsonProperty
    public Optional<List<Type>> getTypes()
    {
        return types;
    }

    @JsonProperty
    public OptionalInt getMaxCompatibleBucketCount()
    {
        return maxCompatibleBucketCount;
    }

    @JsonProperty
    public BucketFunctionType getBucketFunctionType()
    {
        return bucketFunctionType;
    }

    @JsonProperty
    public List<String> getClusteredBy()
    {
        if (clusteredBy.isPresent()) {
            return clusteredBy.get();
        }
        return ImmutableList.of();
    }

    @JsonProperty
    public List<Integer> getClusterCount()
    {
        if (clusterCount.isPresent()) {
            return clusterCount.get();
        }
        return ImmutableList.of();
    }

    @JsonProperty
    public List<String> getDistribution()
    {
        if (distribution.isPresent()) {
            return distribution.get();
        }
        return ImmutableList.of();
    }

    @Override
    public String toString()
    {
        return format(
                "buckets=%s, bucketFunctionType=%s, types=%s",
                bucketCount,
                bucketFunctionType,
                bucketFunctionType.equals(HIVE_COMPATIBLE) ? hiveTypes.get() : types.get());
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
        HivePartitioningHandle that = (HivePartitioningHandle) o;
        return bucketCount == that.bucketCount &&
                bucketFunctionType.equals(that.bucketFunctionType) &&
                Objects.equals(hiveTypes, that.hiveTypes) &&
                Objects.equals(types, that.types);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(bucketCount, bucketFunctionType, hiveTypes, types);
    }
}
