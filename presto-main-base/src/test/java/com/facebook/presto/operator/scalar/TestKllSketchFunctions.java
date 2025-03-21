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

import com.facebook.presto.common.type.SqlVarbinary;
import com.google.common.base.Joiner;
import org.apache.datasketches.common.ArrayOfBooleansSerDe;
import org.apache.datasketches.common.ArrayOfDoublesSerDe;
import org.apache.datasketches.common.ArrayOfLongsSerDe;
import org.apache.datasketches.common.ArrayOfStringsSerDe;
import org.apache.datasketches.kll.KllItemsSketch;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.stream.DoubleStream;
import java.util.stream.LongStream;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;

public class TestKllSketchFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testDoubles()
    {
        KllItemsSketch<Double> sketch = KllItemsSketch.newHeapInstance(Double::compareTo, new ArrayOfDoublesSerDe());
        DoubleStream.iterate(0, i -> i + 1).limit(100).forEach(sketch::update);
        String sketchProjection = getSketchProjection(sketch, "double");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.0 as DOUBLE)"), DOUBLE, 0.0);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)"), DOUBLE, 49.0);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)", false), DOUBLE, 50.0);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(1.0 as DOUBLE)"), DOUBLE, 99.0);

        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(-1 as DOUBLE)"), DOUBLE, 0.0);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(49 as DOUBLE)"), DOUBLE, 0.5);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(50 as DOUBLE)", false), DOUBLE, 0.5);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(99 as DOUBLE)"), DOUBLE, 1.0);
    }

    @Test
    public void testInts()
    {
        KllItemsSketch<Long> sketch = KllItemsSketch.newHeapInstance(Long::compareTo, new ArrayOfLongsSerDe());
        LongStream.iterate(0, i -> i + 1).limit(100).forEach(sketch::update);
        String sketchProjection = getSketchProjection(sketch, "bigint");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.0 as DOUBLE)"), BIGINT, 0L);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)"), BIGINT, 49L);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)", false), BIGINT, 50L);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(1.0 as DOUBLE)"), BIGINT, 99L);

        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(-1 as BIGINT)"), DOUBLE, 0.0);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(49 as BIGINT)"), DOUBLE, 0.5);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(50 as BIGINT)", false), DOUBLE, 0.5);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "CAST(99 as BIGINT)"), DOUBLE, 1.0);
    }

    @Test
    public void testStrings()
    {
        KllItemsSketch<String> sketch = KllItemsSketch.newHeapInstance(String::compareTo, new ArrayOfStringsSerDe());
        Arrays.stream("abcdefghijklmnopqrstuvwxyz".split("")).forEach(sketch::update);
        String sketchProjection = getSketchProjection(sketch, "varchar");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.0 as DOUBLE)"), VARCHAR, "a");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)"), VARCHAR, "m");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)", false), VARCHAR, "n");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(1.0 as DOUBLE)"), VARCHAR, "z");

        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "'1'"), DOUBLE, 0.0);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "'m'"), DOUBLE, 0.5);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "'n'", false), DOUBLE, 0.5);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "'z'"), DOUBLE, 1.0);
    }

    @Test
    public void testBooleans()
    {
        KllItemsSketch<Boolean> sketch = KllItemsSketch.newHeapInstance(Boolean::compareTo, new ArrayOfBooleansSerDe());
        LongStream.iterate(0, i -> i + 1).limit(100).mapToObj(i -> i % 3 == 0).forEach(sketch::update);
        String sketchProjection = getSketchProjection(sketch, "boolean");
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)"), BOOLEAN, false);
        assertFunction(getProjection("sketch_kll_quantile", sketchProjection, "CAST(0.5 as DOUBLE)", false), BOOLEAN, false);

        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "false", false), DOUBLE, 0.0);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "true", false), DOUBLE, 0.66);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "false"), DOUBLE, 0.66);
        assertFunction(getProjection("sketch_kll_rank", sketchProjection, "true"), DOUBLE, 1.0);
    }

    private String getProjection(String functionName, String sketch, Object... args)
    {
        String otherArgs = Joiner.on(",").join(args);
        return String.format("%s(%s)", functionName, Joiner.on(",").join(sketch, otherArgs));
    }

    private String getSketchProjection(KllItemsSketch sketch, String type)
    {
        return getByteArrayProjection(sketch.toByteArray(), type);
    }

    private String getByteArrayProjection(byte[] arr, String type)
    {
        String sqlSerializedSketch = new SqlVarbinary(arr).toString().replaceAll("\\s+", " ");
        return String.format("CAST(X'%s' AS kllsketch(%s))", sqlSerializedSketch, type);
    }
}
