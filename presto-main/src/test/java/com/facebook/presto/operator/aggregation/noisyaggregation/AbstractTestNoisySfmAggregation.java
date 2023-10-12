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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SfmSketch;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;
import com.facebook.presto.type.DoubleOperators;
import com.facebook.presto.type.IntegerOperators;
import com.facebook.presto.type.VarcharOperators;
import io.airlift.slice.Slices;
import org.apache.commons.math3.util.Precision;

import java.util.function.BiFunction;

import static com.facebook.presto.block.BlockAssertions.createDoubleRepeatBlock;
import static com.facebook.presto.block.BlockAssertions.createLongRepeatBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;

/**
 * Parent class for testing noisy_approx_set_sfm and noisy_approx_distinct_sfm.
 * The tests will essentially be the same, but since noisy_approx_set_sfm returns the sketch object itself,
 * we will map this to a cardinality, giving us something equivalent to noisy_approx_distinct_sfm.
 */
abstract class AbstractTestNoisySfmAggregation
{
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = MetadataManager.createTestMetadataManager().getFunctionAndTypeManager();

    protected abstract String getFunctionName();

    protected abstract long getCardinalityFromResult(Object result);

    protected SfmSketch getSketchFromResult(Object result)
    {
        return SfmSketch.deserialize(Slices.wrappedBuffer(((SqlVarbinary) result).getBytes()));
    }

    protected static JavaAggregationFunctionImplementation getAggregator(String functionName, Type... type)
    {
        return FUNCTION_AND_TYPE_MANAGER.getJavaAggregateFunctionImplementation(
                FUNCTION_AND_TYPE_MANAGER.lookupFunction(functionName, fromTypes(type)));
    }

    protected boolean equalCardinalityWithAbsoluteError(Object actual, Object expected, double delta)
    {
        long actualCardinality = getCardinalityFromResult(actual);
        long expectedCardinality = getCardinalityFromResult(expected);
        return Precision.equals(actualCardinality, expectedCardinality, delta);
    }

    /**
     * Run assertion on function with signature F(value, epsilon, numberOfBuckets, precision)
     */
    protected void assertFunction(Block valuesBlock, Type valueType, double epsilon, int numberOfBuckets, int precision, BiFunction<Object, Object, Boolean> assertion, Object expected)
    {
        assertAggregation(
                getAggregator(getFunctionName(), valueType, DOUBLE, BIGINT, BIGINT),
                assertion,
                null,
                new Page(
                        valuesBlock,
                        createDoubleRepeatBlock(epsilon, valuesBlock.getPositionCount()),
                        createLongRepeatBlock(numberOfBuckets, valuesBlock.getPositionCount()),
                        createLongRepeatBlock(precision, valuesBlock.getPositionCount())),
                expected);
    }

    /**
     * Run assertion on function with signature F(value, epsilon, numberOfBuckets)
     */
    protected void assertFunction(Block valuesBlock, Type valueType, double epsilon, int numberOfBuckets, BiFunction<Object, Object, Boolean> assertion, Object expected)
    {
        assertAggregation(
                getAggregator(getFunctionName(), valueType, DOUBLE, BIGINT),
                assertion,
                null,
                new Page(
                        valuesBlock,
                        createDoubleRepeatBlock(epsilon, valuesBlock.getPositionCount()),
                        createLongRepeatBlock(numberOfBuckets, valuesBlock.getPositionCount())),
                expected);
    }

    /**
     * Run assertion on function with signature F(value, epsilon)
     */
    protected void assertFunction(Block valuesBlock, Type valueType, double epsilon, BiFunction<Object, Object, Boolean> assertion, Object expected)
    {
        assertAggregation(
                getAggregator(getFunctionName(), valueType, DOUBLE),
                assertion,
                null,
                new Page(
                        valuesBlock,
                        createDoubleRepeatBlock(epsilon, valuesBlock.getPositionCount())),
                expected);
    }

    /**
     * Assert (approximate) cardinality match on function with signature F(value, epsilon, numberOfBuckets, precision)
     */
    protected void assertCardinality(Block valuesBlock, Type valueType, double epsilon, int numberOfBuckets, int precision, Object expected, long delta)
    {
        assertFunction(valuesBlock, valueType, epsilon, numberOfBuckets, precision,
                (actualValue, expectedValue) -> equalCardinalityWithAbsoluteError(actualValue, expectedValue, delta),
                expected);
    }

    /**
     * Assert (approximate) cardinality match on function with signature F(value, epsilon, numberOfBuckets)
     */
    protected void assertCardinality(Block valuesBlock, Type valueType, double epsilon, int numberOfBuckets, Object expected, long delta)
    {
        assertFunction(valuesBlock, valueType, epsilon, numberOfBuckets,
                (actualValue, expectedValue) -> equalCardinalityWithAbsoluteError(actualValue, expectedValue, delta),
                expected);
    }

    /**
     * Assert (approximate) cardinality match on function with signature F(value, epsilon)
     */
    protected void assertCardinality(Block valuesBlock, Type valueType, double epsilon, Object expected, long delta)
    {
        assertFunction(valuesBlock, valueType, epsilon,
                (actualValue, expectedValue) -> equalCardinalityWithAbsoluteError(actualValue, expectedValue, delta),
                expected);
    }

    protected SfmSketch createLongSketch(int numberOfBuckets, int precision, int start, int end)
    {
        SfmSketch sketch = SfmSketch.create(numberOfBuckets, precision);
        for (int i = start; i < end; i++) {
            sketch.addHash(IntegerOperators.xxHash64(i));
        }
        return sketch;
    }

    protected SfmSketch createDoubleSketch(int numberOfBuckets, int precision, int start, int end)
    {
        SfmSketch sketch = SfmSketch.create(numberOfBuckets, precision);
        for (int i = start; i < end; i++) {
            sketch.addHash(DoubleOperators.xxHash64(i));
        }
        return sketch;
    }

    protected SfmSketch createStringSketch(int numberOfBuckets, int precision, int start, int end)
    {
        SfmSketch sketch = SfmSketch.create(numberOfBuckets, precision);
        for (int i = start; i < end; i++) {
            sketch.addHash(VarcharOperators.xxHash64(Slices.utf8Slice(Long.toString(i))));
        }
        return sketch;
    }

    protected SqlVarbinary toSqlVarbinary(SfmSketch sketch)
    {
        return new SqlVarbinary(sketch.serialize().getBytes());
    }
}
