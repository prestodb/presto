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

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;

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
                Optional.of(types));
    }

    @JsonCreator
    public HivePartitioningHandle(
            @JsonProperty("bucketCount") int bucketCount,
            @JsonProperty("maxCompatibleBucketCount") OptionalInt maxCompatibleBucketCount,
            @JsonProperty("bucketFunctionType") BucketFunctionType bucketFunctionType,
            @JsonProperty("hiveTypes") Optional<List<HiveType>> hiveTypes,
            @JsonProperty("types") Optional<List<Type>> types)
    {
        this.bucketCount = bucketCount;
        this.maxCompatibleBucketCount = maxCompatibleBucketCount;
        this.bucketFunctionType = requireNonNull(bucketFunctionType, "bucketFunctionType is null");
        this.hiveTypes = requireNonNull(hiveTypes, "hiveTypes is null");
        this.types = requireNonNull(types, "types is null");
        checkArgument(bucketFunctionType.equals(HIVE_COMPATIBLE) && hiveTypes.isPresent() && !types.isPresent() ||
                        bucketFunctionType.equals(PRESTO_NATIVE) && !hiveTypes.isPresent() && types.isPresent(),
                "Type list for bucketFunctionType %s is missing or duplicated. hiveTypes: %s, types: %s", bucketFunctionType,
                hiveTypes,
                types);
    }

    @JsonProperty
    public int getBucketCount()
    {
        return bucketCount;
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
