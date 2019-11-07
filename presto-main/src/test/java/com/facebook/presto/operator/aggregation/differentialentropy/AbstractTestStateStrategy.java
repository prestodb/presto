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

import org.testng.annotations.Test;

import java.util.Random;
import java.util.function.Function;

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
        DifferentialEntropyStateStrategy strategy = strategySupplier.apply(2000);
        Random random = new Random(13);
        for (int i = 0; i < 9_999_999; i++) {
            double value = 10 * random.nextFloat();
            if (weighted) {
                strategy.add(value, 1.0);
            }
            else {
                strategy.add(value);
            }
        }
        double expected = Math.log(10) / Math.log(2);
        assertEquals(strategy.calculateEntropy(), expected, 0.1);
    }

    @Test
    public void testNormalDistribution()
    {
        DifferentialEntropyStateStrategy strategy = strategySupplier.apply(200_000);
        Random random = new Random(13);
        double sigma = 0.5;
        for (int i = 0; i < 9_999_999; i++) {
            double value = 5 + sigma * random.nextGaussian();
            if (weighted) {
                strategy.add(value, 1.0);
            }
            else {
                strategy.add(value);
            }
        }
        double expected = 0.5 * Math.log(2 * Math.PI * Math.E * sigma * sigma) / Math.log(2);
        assertEquals(strategy.calculateEntropy(), expected, 0.02);
    }
}
