package com.facebook.presto.hive.clustering;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.clustering.MortonCode;
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

public class HiveClusteringBucketFunction implements BucketFunction
{
    private final List<Integer> clusterCount;
    private final MortonCode mortonCode;
    private final List<String> columnNames;
    private final Optional<List<Type>> types;

    public static BucketFunction createHiveClusteringBucketFunction(
            List<Integer> clusterCount,
            MortonCode mortonCode,
            List<String> columnNames,
            List<Type> types)
    {
        return new HiveClusteringBucketFunction(
                clusterCount,
                mortonCode,
                columnNames,
                Optional.of(types)
        );
    }

    private HiveClusteringBucketFunction(
            List<Integer> clusterCount,
            MortonCode mortonCode,
            List<String> columnNames,
            Optional<List<Type>> types)
    {
        this.clusterCount = clusterCount;
        this.mortonCode = mortonCode;
        this.columnNames = columnNames;
        this.types = requireNonNull(types, "types is null");
    }

    // TODO: Currently this function ignores the bucketCount parameter.
    // This should work for Writers; but not sure if the current reader
    // relies on bucketCount or not.
    public int getBucket(Page page, int position)
    {
        return HiveClustering.getHiveCluster(types.get(), columnNames, page, position, mortonCode);
    }
}
