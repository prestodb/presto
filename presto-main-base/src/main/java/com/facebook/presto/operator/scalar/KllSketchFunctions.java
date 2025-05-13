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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.datasketches.common.ArrayOfBooleansSerDe;
import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.kll.KllItemsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.quantilescommon.QuantileSearchCriteria;

import static com.facebook.presto.common.type.StandardTypes.BIGINT;
import static com.facebook.presto.common.type.StandardTypes.BOOLEAN;
import static com.facebook.presto.common.type.StandardTypes.DOUBLE;
import static com.facebook.presto.common.type.StandardTypes.VARCHAR;
import static java.nio.ByteOrder.LITTLE_ENDIAN;

public class KllSketchFunctions
{
    private KllSketchFunctions()
    {
    }

    @ScalarFunction("sketch_kll_quantile")
    @Description("Calculates the quantile for a given value with the provided inclusivity. If inclusive is true, the given rank includes all quantiles ≤ the quantile directly corresponding to the given rank. If false, the given rank includes all quantiles < the quantile directly corresponding to the given rank.")
    @SqlType(DOUBLE)
    public static double sketchQuantileDouble(
            @SqlType("kllsketch(double)") Slice rawSketch,
            @SqlType(DOUBLE) double rank,
            @SqlType(BOOLEAN) boolean inclusive)

    {
        KllItemsSketch<Double> sketch = KllItemsSketch.wrap(Memory.wrap(rawSketch.toByteBuffer(), LITTLE_ENDIAN), Double::compareTo, new ArrayOfDoublesSerDe());
        return sketch.getQuantile(rank, inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE);
    }

    @ScalarFunction("sketch_kll_quantile")
    @SqlType(DOUBLE)
    public static double sketchQuantileDouble(
            @SqlType("kllsketch(double)") Slice rawSketch,
            @SqlType(DOUBLE) double rank)

    {
        return sketchQuantileDouble(rawSketch, rank, true);
    }

    @ScalarFunction("sketch_kll_quantile")
    @SqlType(BIGINT)
    public static long sketchQuantileBigint(
            @SqlType("kllsketch(bigint)") Slice rawSketch,
            @SqlType(DOUBLE) double rank,
            @SqlType(BOOLEAN) boolean inclusive)

    {
        KllItemsSketch<Long> sketch = KllItemsSketch.wrap(Memory.wrap(rawSketch.toByteBuffer(), LITTLE_ENDIAN), Long::compareTo, new ArrayOfLongsSerDe());
        return sketch.getQuantile(rank, inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE);
    }

    @ScalarFunction("sketch_kll_quantile")
    @SqlType(BIGINT)
    public static long sketchQuantileBigint(
            @SqlType("kllsketch(bigint)") Slice rawSketch,
            @SqlType(DOUBLE) double rank)

    {
        return sketchQuantileBigint(rawSketch, rank, true);
    }

    @ScalarFunction("sketch_kll_quantile")
    @SqlType(VARCHAR)
    public static Slice sketchQuantileString(
            @SqlType("kllsketch(varchar)") Slice rawSketch,
            @SqlType(DOUBLE) double rank,
            @SqlType(BOOLEAN) boolean inclusive)

    {
        KllItemsSketch<String> sketch = KllItemsSketch.wrap(Memory.wrap(rawSketch.toByteBuffer(), LITTLE_ENDIAN), String::compareTo, new ArrayOfStringsSerDe());
        return Slices.utf8Slice(sketch.getQuantile(rank, inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE));
    }

    @ScalarFunction("sketch_kll_quantile")
    @SqlType(VARCHAR)
    public static Slice sketchQuantileString(
            @SqlType("kllsketch(varchar)") Slice rawSketch,
            @SqlType(DOUBLE) double rank)

    {
        return sketchQuantileString(rawSketch, rank, true);
    }

    @ScalarFunction("sketch_kll_quantile")
    @SqlType(BOOLEAN)
    public static boolean sketchQuantileBoolean(
            @SqlType("kllsketch(boolean)") Slice rawSketch,
            @SqlType(DOUBLE) double rank,
            @SqlType(BOOLEAN) boolean inclusive)

    {
        KllItemsSketch<Boolean> sketch = KllItemsSketch.wrap(Memory.wrap(rawSketch.toByteBuffer(), LITTLE_ENDIAN), Boolean::compareTo, new ArrayOfBooleansSerDe());
        return sketch.getQuantile(rank, inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE);
    }

    @ScalarFunction("sketch_kll_quantile")
    @SqlType(BOOLEAN)
    public static boolean sketchQuantileBoolean(
            @SqlType("kllsketch(boolean)") Slice rawSketch,
            @SqlType(DOUBLE) double rank)

    {
        return sketchQuantileBoolean(rawSketch, rank, true);
    }

    @ScalarFunction("sketch_kll_rank")
    @Description("Calculates the rank of a quantile; An estimate of the value which occurs at a particular quantile in the distribution")
    @SqlType(DOUBLE)
    public static double sketchRank(
            @SqlType("kllsketch(double)") Slice rawSketch,
            @SqlType(DOUBLE) double quantile,
            @SqlType(BOOLEAN) boolean inclusive)

    {
        KllItemsSketch<Double> sketch = KllItemsSketch.wrap(Memory.wrap(rawSketch.toByteBuffer(), LITTLE_ENDIAN), Double::compareTo, new ArrayOfDoublesSerDe());
        return sketch.getRank(quantile, inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE);
    }

    @ScalarFunction("sketch_kll_rank")
    @SqlType(DOUBLE)
    public static double sketchRank(
            @SqlType("kllsketch(double)") Slice rawSketch,
            @SqlType(DOUBLE) double quantile)

    {
        return sketchRank(rawSketch, quantile, true);
    }

    @ScalarFunction("sketch_kll_rank")
    @SqlType(DOUBLE)
    public static double sketchRank(
            @SqlType("kllsketch(bigint)") Slice rawSketch,
            @SqlType(BIGINT) long quantile,
            @SqlType(BOOLEAN) boolean inclusive)

    {
        KllItemsSketch<Long> sketch = KllItemsSketch.wrap(Memory.wrap(rawSketch.toByteBuffer(), LITTLE_ENDIAN), Long::compareTo, new ArrayOfLongsSerDe());
        return sketch.getRank(quantile, inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE);
    }

    @ScalarFunction("sketch_kll_rank")
    @SqlType(DOUBLE)
    public static double sketchRank(
            @SqlType("kllsketch(bigint)") Slice rawSketch,
            @SqlType(BIGINT) long quantile)

    {
        return sketchRank(rawSketch, quantile, true);
    }

    @ScalarFunction("sketch_kll_rank")
    @SqlType(DOUBLE)
    public static double sketchRank(
            @SqlType("kllsketch(varchar)") Slice rawSketch,
            @SqlType(VARCHAR) Slice rank,
            @SqlType(BOOLEAN) boolean inclusive)

    {
        KllItemsSketch<String> sketch = KllItemsSketch.wrap(Memory.wrap(rawSketch.toByteBuffer(), LITTLE_ENDIAN), String::compareTo, new ArrayOfStringsSerDe());
        return sketch.getRank(rank.toStringUtf8(), inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE);
    }

    @ScalarFunction("sketch_kll_rank")
    @SqlType(DOUBLE)
    public static double sketchRank(
            @SqlType("kllsketch(varchar)") Slice rawSketch,
            @SqlType(VARCHAR) Slice quantile)

    {
        return sketchRank(rawSketch, quantile, true);
    }

    @ScalarFunction("sketch_kll_rank")
    @SqlType(DOUBLE)
    public static double sketchRank(
            @SqlType("kllsketch(boolean)") Slice rawSketch,
            @SqlType(BOOLEAN) boolean rank,
            @SqlType(BOOLEAN) boolean inclusive)

    {
        KllItemsSketch<Boolean> sketch = KllItemsSketch.wrap(Memory.wrap(rawSketch.toByteBuffer(), LITTLE_ENDIAN), Boolean::compareTo, new ArrayOfBooleansSerDe());
        return sketch.getRank(rank, inclusive ? QuantileSearchCriteria.INCLUSIVE : QuantileSearchCriteria.EXCLUSIVE);
    }

    @ScalarFunction("sketch_kll_rank")
    @SqlType(DOUBLE)
    public static double sketchRank(
            @SqlType("kllsketch(boolean)") Slice rawSketch,
            @SqlType(BOOLEAN) boolean quantile)

    {
        return sketchRank(rawSketch, quantile, true);
    }
}
