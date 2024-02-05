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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.SfmSketch;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createDoubleSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createLongSequenceBlock;
import static com.facebook.presto.block.BlockAssertions.createStringSequenceBlock;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;

public class TestNoisyApproximateCountDistinctSfmAggregation
        extends AbstractTestNoisySfmAggregation
{
    protected String getFunctionName()
    {
        return "noisy_approx_distinct_sfm";
    }

    protected long getCardinalityFromResult(Object result)
    {
        return new Long(result.toString());
    }

    @Test
    public void testNonPrivateIntegerCount()
    {
        Block valuesBlock = createLongSequenceBlock(1, 100_000);
        // These estimates are deterministic (no privacy).
        assertCardinality(valuesBlock, BIGINT, SfmSketch.NON_PRIVATE_EPSILON, 99_466, 0);
        assertCardinality(valuesBlock, BIGINT, SfmSketch.NON_PRIVATE_EPSILON, 8192, 100_219, 0);
        assertCardinality(valuesBlock, BIGINT, SfmSketch.NON_PRIVATE_EPSILON, 2048, 32, 100_102, 0);
    }

    @Test
    public void testPrivateIntegerCount()
    {
        Block valuesBlock = createLongSequenceBlock(1, 100_000);
        // These estimates are random, but not too noisy.
        assertCardinality(valuesBlock, BIGINT, 8, 100_000, 25_000);
        assertCardinality(valuesBlock, BIGINT, 8, 8192, 100_000, 25_000);
        assertCardinality(valuesBlock, BIGINT, 8, 2048, 32, 100_000, 25_000);
    }

    @Test
    public void testNonPrivateDoubleCount()
    {
        Block valuesBlock = createDoubleSequenceBlock(1, 100_000);
        // These estimates are deterministic (no privacy).
        assertCardinality(valuesBlock, DOUBLE, SfmSketch.NON_PRIVATE_EPSILON, 99_670, 0);
        assertCardinality(valuesBlock, DOUBLE, SfmSketch.NON_PRIVATE_EPSILON, 8192, 100_078, 0);
        assertCardinality(valuesBlock, DOUBLE, SfmSketch.NON_PRIVATE_EPSILON, 2048, 32, 98_350, 0);
    }

    @Test
    public void testPrivateDoubleCount()
    {
        Block valuesBlock = createDoubleSequenceBlock(1, 100_000);
        // These estimates are random, but not too noisy.
        assertCardinality(valuesBlock, DOUBLE, 8, 100_000, 25_000);
        assertCardinality(valuesBlock, DOUBLE, 8, 8192, 100_000, 25_000);
        assertCardinality(valuesBlock, DOUBLE, 8, 2048, 32, 100_000, 25_000);
    }

    @Test
    public void testNonPrivateStringCount()
    {
        Block valuesBlock = createStringSequenceBlock(1, 100_000);
        // These estimates are deterministic.
        assertCardinality(valuesBlock, VARCHAR, SfmSketch.NON_PRIVATE_EPSILON, 100_190, 0);
        assertCardinality(valuesBlock, VARCHAR, SfmSketch.NON_PRIVATE_EPSILON, 8192, 99_982, 0);
        assertCardinality(valuesBlock, VARCHAR, SfmSketch.NON_PRIVATE_EPSILON, 2048, 32, 100_773, 0);
    }

    @Test
    public void testPrivateStringCount()
    {
        Block valuesBlock = createStringSequenceBlock(1, 100_000);
        // These estimates are random, but not too noisy.
        assertCardinality(valuesBlock, VARCHAR, 8, 100_000, 25_000);
        assertCardinality(valuesBlock, VARCHAR, 8, 8192, 100_000, 25_000);
        assertCardinality(valuesBlock, VARCHAR, 8, 2048, 32, 100_000, 25_000);
    }
}
