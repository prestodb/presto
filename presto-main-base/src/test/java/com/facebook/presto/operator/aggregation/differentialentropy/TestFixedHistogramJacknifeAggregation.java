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
package com.facebook.presto.operator.aggregation.differentialentropy;

import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.aggregation;
import static com.facebook.presto.operator.aggregation.differentialentropy.DifferentialEntropyStateStrategy.FIXED_HISTOGRAM_JACKNIFE_METHOD_NAME;

public class TestFixedHistogramJacknifeAggregation
        extends AbstractTestFixedHistogramAggregation
{
    public TestFixedHistogramJacknifeAggregation()
    {
        super(FIXED_HISTOGRAM_JACKNIFE_METHOD_NAME);
    }

    @Test(
            expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "In differential_entropy UDF, bucket count must be non-negative: -200")
    public void testIllegalBucketCount()
    {
        aggregation(
                getFunction(),
                createLongsBlock(-200),
                createDoublesBlock(0.1),
                createDoublesBlock(0.2),
                createStringsBlock(method),
                createDoublesBlock(0.0),
                createDoublesBlock(0.2));
    }

    @Test(
            expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "In differential_entropy UDF, weight must be non-negative: -0.2")
    public void testNegativeWeight()
    {
        aggregation(
                getFunction(),
                createLongsBlock(200),
                createDoublesBlock(0.1),
                createDoublesBlock(-0.2),
                createStringsBlock(method),
                createDoublesBlock(0.0),
                createDoublesBlock(0.2));
    }

    @Test(
            expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "In differential_entropy UDF, sample must be at least min: sample=-100.0, min=0.0")
    public void testTooSmallSample()
    {
        aggregation(
                getFunction(),
                createLongsBlock(200),
                createDoublesBlock(-100.0),
                createDoublesBlock(0.2),
                createStringsBlock(method),
                createDoublesBlock(0.0),
                createDoublesBlock(0.2));
    }

    @Test(
            expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "In differential_entropy UDF, sample must be at most max: sample=300.0, max=0.2")
    public void testTooLargeSample()
    {
        aggregation(
                getFunction(),
                createLongsBlock(200),
                createDoublesBlock(300.0),
                createDoublesBlock(0.2),
                createStringsBlock(method),
                createDoublesBlock(0.0),
                createDoublesBlock(0.2));
    }

    @Test(
            expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "In differential_entropy UDF, min must be larger than max: min=0.2, max=0.1")
    public void testIllegalMinMax()
    {
        aggregation(
                getFunction(),
                createLongsBlock(200),
                createDoublesBlock(0.1),
                createDoublesBlock(0.2),
                createStringsBlock(method),
                createDoublesBlock(0.2),
                createDoublesBlock(0.1));
    }

    @Override
    public Double getExpectedValue(int start, int length)
    {
        List<Double> samples = new ArrayList<>();
        List<Double> weights = new ArrayList<>();
        generateSamplesAndWeights(start, length, samples, weights);

        double entropy = samples.size() * calculateEntropy(samples, weights);
        for (int i = 0; i < samples.size(); ++i) {
            List<Double> subSamples = new ArrayList<>(samples);
            subSamples.remove(i);
            List<Double> subWeights = new ArrayList<>(weights);
            subWeights.remove(i);

            double holdOutEntropy = (samples.size() - 1) * calculateEntropy(subSamples, subWeights) / samples.size();
            entropy -= holdOutEntropy;
        }
        return entropy;
    }
}
