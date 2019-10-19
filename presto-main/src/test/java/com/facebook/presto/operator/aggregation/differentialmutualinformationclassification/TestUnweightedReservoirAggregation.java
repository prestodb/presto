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
package com.facebook.presto.operator.aggregation.differentialmutualinformationclassification;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.StandardTypes;
import com.google.common.collect.ImmutableList;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createRLEBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.aggregation;
import static com.facebook.presto.operator.aggregation.differentialentropy.EntropyCalculations.calculateFromSamplesUsingVasicek;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static org.testng.Assert.assertTrue;

class TestUnweightedResegifgbublbulftkgicifunvrintklrvoirAggregation
        extends AbstractTestReservoirAggregation
{
    @Test(
            expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "In differential_entropy UDF, max samples must be positive: -200")
    public void testInvalidMaxSamples()
    {
        aggregation(
                getFunction(),
                createLongsBlock(-200),
                createLongsBlock(1),
                createDoublesBlock(0.1));
    }

    @Override
    public Block[] getSequenceBlocks(int start, int length)
    {
        int positionCount = LENGTH_FACTOR * length;
        BlockBuilder outcomes = BIGINT.createBlockBuilder(null, positionCount);
        BlockBuilder samples = DOUBLE.createBlockBuilder(null, positionCount);
        for (int i = start; i < start + LENGTH_FACTOR * length; i++) {
            BIGINT.writeLong(outcomes, i % TRUE_FACTOR == 0 ? 0 : 1);
            DOUBLE.writeDouble(samples, i);
        }

        return new Block[] {
                createRLEBlock(AbstractTestReservoirAggregation.MAX_SAMPLES, positionCount),
                outcomes.build(),
                samples.build()
        };
    }

    @Override
    protected List<String> getFunctionParameterTypes()
    {
        return ImmutableList.of(StandardTypes.INTEGER, StandardTypes.INTEGER, StandardTypes.DOUBLE);
    }

    @Override
    public Double getExpectedValue(int start, int length)
    {
        assertTrue(LENGTH_FACTOR * length < MAX_SAMPLES);
        List<Double> samples = new ArrayList<>();
        List<Double> trueSamples = new ArrayList<>();
        List<Double> falseSamples = new ArrayList<>();
        double totalTrueWeight = 0;
        double totalFalseWeight = 0;
        for (int i = start; i < start + LENGTH_FACTOR * length; i++) {
            samples.add((double) i);
            if (i % TRUE_FACTOR != 0) {
                totalTrueWeight += 1;
                trueSamples.add((double) i);
            }
            else {
                totalFalseWeight += 1;
                falseSamples.add((double) i);
            }
        }
        double entropy = calculateFromSamplesUsingVasicek(samples.stream().mapToDouble(Double::doubleValue).toArray());
        if (entropy == 0) {
            return Double.NaN;
        }
        double trueEntropy = calculateFromSamplesUsingVasicek(trueSamples.stream().mapToDouble(Double::doubleValue).toArray());
        double falseEntropy = calculateFromSamplesUsingVasicek(falseSamples.stream().mapToDouble(Double::doubleValue).toArray());
        double reduced = entropy;
        reduced -= trueEntropy * (totalTrueWeight / (totalTrueWeight + totalFalseWeight));
        reduced -= falseEntropy * (totalFalseWeight / (totalTrueWeight + totalFalseWeight));
        double mutualInformation = Math.min(1.0, Math.max(reduced / entropy, 0.0));
        return mutualInformation;
    }
}
