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

import com.facebook.presto.operator.aggregation.differentialentropy.DifferentialEntropyStateStrategy;
import com.facebook.presto.spi.PrestoException;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.block.BlockAssertions.createLongsBlock;
import static com.facebook.presto.block.BlockAssertions.createStringsBlock;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.aggregation;

public class TestFixedHistogramJacknifeAggregation
        extends AbstractTestFixedHistogramAggregation
{
    public TestFixedHistogramJacknifeAggregation()
    {
        super(DifferentialEntropyStateStrategy.FIXED_HISTOGRAM_JACKNIFE_METHOD_NAME);
    }

    @Test(
            expectedExceptions = PrestoException.class,
            expectedExceptionsMessageRegExp = "In differential_entropy UDF, bucket count must be non-negative: -200")
    public void testIllegalBucketCount()
    {
        aggregation(
                getFunction(),
                createLongsBlock(-200),
                createLongsBlock(1),
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
                createLongsBlock(1),
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
                createLongsBlock(1),
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
                createLongsBlock(1),
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
                createLongsBlock(1),
                createDoublesBlock(0.1),
                createDoublesBlock(0.2),
                createStringsBlock(method),
                createDoublesBlock(0.2),
                createDoublesBlock(0.1));
    }

    @Override
    public Double getExpectedValue(int start, int length)
    {
        List<Integer> outcomes = new ArrayList<>();
        List<Double> samples = new ArrayList<>();
        List<Double> weights = new ArrayList<>();
        generateOutcomesSamplesAndWeights(start, length, outcomes, samples, weights);
        List<Double> trueSamples = new ArrayList<>();
        List<Double> trueWeights = new ArrayList<>();
        List<Double> falseSamples = new ArrayList<>();
        List<Double> falseWeights = new ArrayList<>();
        double totalTrueWeight = 0;
        double totalFalseWeight = 0;
        for (int i = 0; i < samples.size(); i++) {
            if (outcomes.get(i) == 1) {
                totalTrueWeight += weights.get(i);
                trueSamples.add(samples.get(i));
                trueWeights.add(weights.get(i));
            }
            else {
                totalFalseWeight += weights.get(i);
                falseSamples.add(samples.get(i));
                falseWeights.add(weights.get(i));
            }
        }
        double entropy = calculateEntropy(samples, weights);
        double reduced = entropy;
        double positive = calculateEntropy(trueSamples, trueWeights);
        reduced -= positive * (totalTrueWeight / (totalTrueWeight + totalFalseWeight));
        double negative = calculateEntropy(falseSamples, falseWeights);
        reduced -= negative * (totalFalseWeight / (totalTrueWeight + totalFalseWeight));
        double mutualInformation = Math.min(1.0, Math.max(reduced / entropy, 0.0));
        return mutualInformation;
    }

    protected static double calculateEntropy(List<Double> samples, List<Double> weights)
    {
        double entropy = samples.size() * AbstractTestFixedHistogramAggregation.calculateEntropy(samples, weights);
        for (int i = 0; i < samples.size(); ++i) {
            List<Double> subSamples = new ArrayList<>(samples);
            subSamples.remove(i);
            List<Double> subWeights = new ArrayList<>(weights);
            subWeights.remove(i);

            double holdOutEntropy = (samples.size() - 1) * AbstractTestFixedHistogramAggregation.calculateEntropy(subSamples, subWeights) / samples.size();
            entropy -= holdOutEntropy;
        }
        return entropy;
    }
}
