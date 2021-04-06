package com.facebook.presto.hive.clustering;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.hive.BucketFunctionType;
import com.facebook.presto.hive.HiveBucketFunction;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.spi.BucketFunction;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.hive.BucketFunctionType.HIVE_CLUSTERING;
import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class HiveClusteringFunction implements BucketFunction
{
    private final MortonCode mortonCode;
    private final BucketFunctionType bucketFunctionType;
    private final List<String> columnNames;
    private final Optional<List<TypeInfo>> typeInfos;
    private final Optional<List<Type>> types;

    public static HiveClusteringFunction createHiveClusteringFunction(
            MortonCode mortonCode,
            List<String> columnNames,
            List<HiveType> hiveTypes)
    {
        return new HiveClusteringFunction(
                mortonCode,
                columnNames,
                HIVE_CLUSTERING,
                Optional.of(hiveTypes),
                Optional.empty()
        );
    }

    private HiveClusteringFunction(
            MortonCode mortonCode,
            List<String> columnNames,
            BucketFunctionType bucketFunctionType,
            Optional<List<HiveType>> hiveTypes,
            Optional<List<Type>> types)
    {
        this.mortonCode = mortonCode;
        this.columnNames = columnNames;
        this.bucketFunctionType = requireNonNull(bucketFunctionType, "bucketFunctionType is null");
        checkArgument(bucketFunctionType.equals(HIVE_CLUSTERING) && hiveTypes.isPresent() && types.isPresent(),
                "Incorrect bucketFunctionType " + bucketFunctionType);
        this.typeInfos = hiveTypes.map(list -> list.stream()
                .map(HiveType::getTypeInfo)
                .collect(toImmutableList()));
        this.types = requireNonNull(types, "types is null");
    }

    public int getBucket(Page page, int position)
    {
        return HiveClustering.getHiveCluster(typeInfos.get(), columnNames, page, position, mortonCode);
    }
}
