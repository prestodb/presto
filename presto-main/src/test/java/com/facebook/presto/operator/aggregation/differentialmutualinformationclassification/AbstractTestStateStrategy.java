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
import org.testng.annotations.Test;

import java.util.Random;
import java.util.function.Function;

import static com.google.common.base.Preconditions.checkArgument;
import static org.testng.Assert.assertEquals;

abstract class AbstractTestStateStrategy
{
    protected static final double MIN = 0.0;
    protected static final double MAX = 10.0;

    private final Function<Integer, DifferentialEntropyStateStrategy> strategySupplier;
    private final boolean weighted;

    protected AbstractTestStateStrategy(
            Function<Integer, DifferentialEntropyStateStrategy> strategySupplier,
            boolean weighted)
    {
        this.strategySupplier = strategySupplier;
        this.weighted = weighted;
    }

    @Test
    public void testUniformDistribution()
    {
        assertEquals(calculateUniform(1), 1.0, 0.01);
    }

    private double calculateUniform(double noise)
    {
        checkArgument(noise >= 0 && noise <= 1);
        int size = 2_000;
        DifferentialEntropyStateStrategy strategy = strategySupplier.apply(size);
        DifferentialEntropyStateStrategy trueStrategy = strategySupplier.apply(size);
        DifferentialEntropyStateStrategy falseStrategy = strategySupplier.apply(size);
        Random random = new Random(13);
        for (int i = 0; i < 9_999_999; i++) {
            int outcome = random.nextBoolean() ? 1 : 0;
            double value = MIN + (MAX - MIN) / 2 * random.nextFloat();
            if (outcome == 1) {
                value += (MAX - MIN) / 2;
            }
            add(strategy, value);
            if (outcome == 1) {
                add(trueStrategy, value);
            }
            else {
                add(falseStrategy, value);
            }
        }
        double entropy = strategy.calculateEntropy();
        double trueEntropy = trueStrategy.calculateEntropy();
        double falseEntropy = falseStrategy.calculateEntropy();
        double totalTrueWeight = trueStrategy.getTotalPopulationWeight();
        double totalFalseWeight = falseStrategy.getTotalPopulationWeight();
        double reduced = entropy;
        reduced -= trueEntropy * (totalTrueWeight / (totalTrueWeight + totalFalseWeight));
        reduced -= falseEntropy * (totalFalseWeight / (totalTrueWeight + totalFalseWeight));
        double mutualInformation = Math.min(1.0, Math.max(reduced / entropy, 0.0));
        return mutualInformation;
    }

    private void add(DifferentialEntropyStateStrategy strategy, double value)
    {
        if (weighted) {
            strategy.add(value, 1.0);
        }
        else {
            strategy.add(value);
        }
    }
}
