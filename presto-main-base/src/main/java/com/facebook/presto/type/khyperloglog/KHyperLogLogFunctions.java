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

package com.facebook.presto.type.khyperloglog;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import io.airlift.slice.Slice;

import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;

public final class KHyperLogLogFunctions
{
    private KHyperLogLogFunctions()
    {
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long cardinality(@SqlType(KHyperLogLogType.NAME) Slice khll)
    {
        return KHyperLogLog.newInstance(khll).cardinality();
    }

    @ScalarFunction
    @SqlType(StandardTypes.BIGINT)
    public static long intersectionCardinality(@SqlType(KHyperLogLogType.NAME) Slice slice1, @SqlType(KHyperLogLogType.NAME) Slice slice2)
    {
        KHyperLogLog khll1 = KHyperLogLog.newInstance(slice1);
        KHyperLogLog khll2 = KHyperLogLog.newInstance(slice2);

        if (khll1.isExact() && khll2.isExact()) {
            return KHyperLogLog.exactIntersectionCardinality(khll1, khll2);
        }

        long lowestCardinality = Math.min(khll1.cardinality(), khll2.cardinality());
        double jaccard = KHyperLogLog.jaccardIndex(khll1, khll2);
        KHyperLogLog setUnion = KHyperLogLog.merge(khll1, khll2);
        long result = Math.round(jaccard * setUnion.cardinality());

        // When one of the sets is much smaller than the other and approaches being a true
        // subset of the other, the computed cardinality may exceed the cardinality estimate
        // of the smaller set. When this happens the cardinality of the smaller set is obviously
        // a better estimate of the one computed with the Jaccard Index.
        return Math.min(result, lowestCardinality);
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double jaccardIndex(@SqlType(KHyperLogLogType.NAME) Slice slice1, @SqlType(KHyperLogLogType.NAME) Slice slice2)
    {
        KHyperLogLog khll1 = KHyperLogLog.newInstance(slice1);
        KHyperLogLog khll2 = KHyperLogLog.newInstance(slice2);

        return KHyperLogLog.jaccardIndex(khll1, khll2);
    }

    @ScalarFunction
    @SqlType("map(bigint,double)")
    public static Block uniquenessDistribution(@TypeParameter("map<bigint,double>") Type mapType, @SqlType(KHyperLogLogType.NAME) Slice slice)
    {
        KHyperLogLog khll = KHyperLogLog.newInstance(slice);
        return uniquenessDistribution(mapType, slice, khll.getMinhashSize());
    }

    @ScalarFunction
    @SqlType("map(bigint,double)")
    public static Block uniquenessDistribution(@TypeParameter("map<bigint,double>") Type mapType, @SqlType(KHyperLogLogType.NAME) Slice slice, @SqlType(StandardTypes.BIGINT) long histogramSize)
    {
        KHyperLogLog khll = KHyperLogLog.newInstance(slice);

        BlockBuilder blockBuilder = mapType.createBlockBuilder(null, 1);
        BlockBuilder singleMapBlockBuilder = blockBuilder.beginBlockEntry();
        for (Map.Entry<Long, Double> entry : khll.uniquenessDistribution(histogramSize).entrySet()) {
            BIGINT.writeLong(singleMapBlockBuilder, entry.getKey());
            DOUBLE.writeDouble(singleMapBlockBuilder, entry.getValue());
        }
        blockBuilder.closeEntry();

        return (Block) mapType.getObject(blockBuilder, 0);
    }

    @ScalarFunction
    @SqlType(StandardTypes.DOUBLE)
    public static double reidentificationPotential(@SqlType(KHyperLogLogType.NAME) Slice khll, @SqlType(StandardTypes.BIGINT) long threshold)
    {
        return KHyperLogLog.newInstance(khll).reidentificationPotential(threshold);
    }

    @ScalarFunction
    @SqlType(KHyperLogLogType.NAME)
    @SqlNullable
    public static Slice mergeKhll(@SqlType("array(KHyperLogLog)") Block block)
    {
        if (block.getPositionCount() == 0) {
            return null;
        }

        KHyperLogLog merged = null;
        int firstNonNullIndex = 0;

        while (firstNonNullIndex < block.getPositionCount() && block.isNull(firstNonNullIndex)) {
            firstNonNullIndex++;
        }

        if (firstNonNullIndex == block.getPositionCount()) {
            return null;
        }

        Slice initialSlice = block.getSlice(firstNonNullIndex, 0, block.getSliceLength(firstNonNullIndex));
        merged = KHyperLogLog.newInstance(initialSlice);

        for (int i = firstNonNullIndex; i < block.getPositionCount(); i++) {
            Slice currentSlice = block.getSlice(i, 0, block.getSliceLength(i));
            if (!block.isNull(i)) {
                merged = KHyperLogLog.merge(merged, KHyperLogLog.newInstance(currentSlice));
            }
        }

        return merged.serialize();
    }
}
