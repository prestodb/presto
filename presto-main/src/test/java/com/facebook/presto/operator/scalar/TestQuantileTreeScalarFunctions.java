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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.RandomizationStrategy;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree.QuantileTree;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree.TestQuantileTreeSeededRandomizationStrategy;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.operator.scalar.QuantileTreeScalarFunctions.cardinality;
import static com.facebook.presto.operator.scalar.QuantileTreeScalarFunctions.emptyQuantileTree;
import static com.facebook.presto.operator.scalar.QuantileTreeScalarFunctions.ensureNoise;
import static com.facebook.presto.operator.scalar.QuantileTreeScalarFunctions.valueAtQuantile;
import static com.facebook.presto.operator.scalar.QuantileTreeScalarFunctions.valuesAtQuantiles;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestQuantileTreeScalarFunctions
{
    @Test
    public void testCardinality()
    {
        QuantileTree tree = new QuantileTree(0, 10000, 1024, 2, 5, 272);
        int n = 123;
        for (int i = 0; i < n; i++) {
            tree.add(i); // add n values
        }

        assertEquals(cardinality(tree.serialize()), n);
    }

    @Test
    public void testEmptyQuantileTreeNonPrivate()
    {
        QuantileTree nonPrivateTree = QuantileTree.deserialize(emptyQuantileTree(Double.POSITIVE_INFINITY, 1, -50, 50));
        assertFalse(nonPrivateTree.isPrivacyEnabled());
        assertEquals(nonPrivateTree.cardinality(), 0);
    }

    @Test
    public void testEmptyQuantileTreePrivateDeterministic()
    {
        QuantileTree privateTree = QuantileTree.deserialize(emptyQuantileTree(2, 1e-6, -50, 50, new TestQuantileTreeSeededRandomizationStrategy(10)));
        assertTrue(privateTree.isPrivacyEnabled());
        assertEquals(privateTree.cardinality(), 0, 20.0);
    }

    @Test
    public void testEmptyQuantileTreePrivateRandom()
    {
        // This test uses the SecureRandomizationStrategy, so we check the privacy but not the cardinality.
        QuantileTree privateTree = QuantileTree.deserialize(emptyQuantileTree(2, 1e-6, -50, 50));
        assertTrue(privateTree.isPrivacyEnabled());
    }

    @Test
    public void testEnsureNoiseOnNoiselessTree()
    {
        QuantileTree tree = new QuantileTree(-10, 10, 1024, 2, 5, 272);
        for (int i = 0; i < 5; i++) {
            tree.add(i);
        }

        assertFalse(tree.isPrivacyEnabled());
        double epsilon = 1;
        double delta = 1e-6;

        QuantileTree returnedTree = QuantileTree.deserialize(ensureNoise(tree.serialize(), epsilon, delta));

        assertTrue(returnedTree.isPrivacyEnabled());
        assertEquals(returnedTree.getRho(), QuantileTree.getRhoForEpsilonDelta(epsilon, delta));
    }

    @Test
    public void testEnsureNoiseOnNoisyTree()
    {
        RandomizationStrategy randomizationStrategy = new TestQuantileTreeSeededRandomizationStrategy(1);
        QuantileTree tree = new QuantileTree(-10, 10, 1024, 2, 5, 272);
        for (int i = 0; i < 5; i++) {
            tree.add(i);
        }
        tree.enablePrivacy(0.5, randomizationStrategy);

        assertTrue(tree.isPrivacyEnabled());
        assertEquals(tree.getRho(), 0.5);

        double epsilon = 1;
        double delta = 1e-6;

        QuantileTree returnedTree = QuantileTree.deserialize(ensureNoise(tree.serialize(), epsilon, delta));

        assertTrue(returnedTree.isPrivacyEnabled());
        assertEquals(returnedTree.getRho(), QuantileTree.getRhoForEpsilonDelta(epsilon, delta));
    }

    @Test
    public void testValueAtQuantile()
    {
        RandomizationStrategy randomizationStrategy = new TestQuantileTreeSeededRandomizationStrategy(1);
        QuantileTree tree = new QuantileTree(-0.1, 1000.1, 1024, 2, 5, 272);
        tree.enablePrivacy(0.2, randomizationStrategy);
        for (int i = 0; i < 1_000; i++) {
            tree.add(randomizationStrategy.nextInt(100));
        }
        Slice serialized = tree.serialize();
        assertEquals(valueAtQuantile(serialized, 0.5), 50, 5.0);
    }

    @Test
    public void testValuesAtQuantiles()
    {
        RandomizationStrategy randomizationStrategy = new TestQuantileTreeSeededRandomizationStrategy(1);
        QuantileTree tree = new QuantileTree(-0.1, 1000.1, 1024, 2, 5, 272);
        tree.enablePrivacy(0.2, randomizationStrategy);
        for (int i = 0; i < 1_000; i++) {
            tree.add(randomizationStrategy.nextInt(100));
        }
        Slice serialized = tree.serialize();

        Block resultBlock = valuesAtQuantiles(serialized, createDoublesBlock(0.1, 0.25, 0.33, 0.5, 0.75, 0.9, 0.99));
        double[] resultArray = new double[resultBlock.getPositionCount()];
        for (int i = 0; i < resultBlock.getPositionCount(); i++) {
            resultArray[i] = DOUBLE.getDouble(resultBlock, i);
        }
        double[] expected = {10, 25, 33, 50, 75, 90, 99};
        assertEquals(resultArray, expected, 5.0);
    }
}
