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

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.BucketFunction;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.BucketFunctionType.PRESTO_NATIVE;
import static com.facebook.presto.hive.HiveBucketing.getHiveBucket;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class HiveBucketFunction
        implements BucketFunction
{
    private final int bucketCount;
    private final BucketFunctionType bucketFunctionType;
    private final Optional<List<TypeInfo>> typeInfos;
    private final Optional<List<Type>> types;

    public static HiveBucketFunction createHiveCompatibleBucketFunction(
            int bucketCount,
            List<HiveType> hiveTypes)
    {
        return new HiveBucketFunction(bucketCount, HIVE_COMPATIBLE, Optional.of(hiveTypes), Optional.empty());
    }

    public static HiveBucketFunction createPrestoNativeBucketFunction(
            int bucketCount,
            List<Type> types)
    {
        return new HiveBucketFunction(bucketCount, PRESTO_NATIVE, Optional.empty(), Optional.of(types));
    }

    private HiveBucketFunction(
            int bucketCount,
            BucketFunctionType bucketFunctionType,
            Optional<List<HiveType>> hiveTypes,
            Optional<List<Type>> types)
    {
        this.bucketCount = bucketCount;
        this.bucketFunctionType = requireNonNull(bucketFunctionType, "bucketFunctionType is null");
        checkArgument(bucketFunctionType.equals(BucketFunctionType.HIVE_COMPATIBLE) && hiveTypes.isPresent() ||
                        bucketFunctionType.equals(BucketFunctionType.PRESTO_NATIVE) && types.isPresent(),
                "The corresponding type list is not present for bucketFunctionType " + bucketFunctionType);
        this.typeInfos = hiveTypes.map(list -> list.stream()
                .map(HiveType::getTypeInfo)
                .collect(toImmutableList()));
        this.types = requireNonNull(types, "types is null");
    }

    @Override
    public int getBucket(Page page, int position)
    {
        switch (bucketFunctionType) {
            case HIVE_COMPATIBLE:
                return getHiveBucket(bucketCount, typeInfos.get(), page, position);
            case PRESTO_NATIVE:
                return HiveBucketing.getBucket(bucketCount, types.get(), page, position);
            default:
                throw new IllegalArgumentException("Unsupported bucket function type " + bucketFunctionType);
        }
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("bucketCount", bucketCount)
                .add("bucketFunctionType", bucketFunctionType)
                .add("types", bucketFunctionType.equals(HIVE_COMPATIBLE) ? typeInfos.get() : types.get())
                .toString();
    }
}
