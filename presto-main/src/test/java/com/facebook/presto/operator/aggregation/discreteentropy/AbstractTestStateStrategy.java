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
package com.facebook.presto.operator.aggregation.discreteentropy;

import org.testng.annotations.Test;

import java.util.Random;
import java.util.function.Supplier;

import static org.testng.Assert.assertEquals;

public class AbstractTestStateStrategy
{
    Supplier<DiscreteEntropyStateStrategy> strategySupplier;
    boolean weighted;

    protected AbstractTestStateStrategy(Supplier<DiscreteEntropyStateStrategy> strategySupplier, boolean weighted)
    {
        this.strategySupplier = strategySupplier;
        this.weighted = weighted;
    }

    @Test
    public void testUniformDistribution()
    {
        int size = 10;
        DiscreteEntropyStateStrategy strategy = strategySupplier.get();
        Random random = new Random(13);
        for (int i = 0; i < 9_999_999; i++) {
            int value = random.nextInt(size);
            if (weighted) {
                strategy.add(value, random.nextInt(10));
            }
            else {
                strategy.add(value);
            }
        }
        double expected = Math.log(10) / Math.log(2);
        assertEquals(strategy.calculateEntropy(), expected, 0.1);
    }
}
