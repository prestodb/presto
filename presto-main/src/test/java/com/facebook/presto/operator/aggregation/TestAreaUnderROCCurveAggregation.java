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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.block.Block;
import org.testng.annotations.Test;

import static com.facebook.presto.block.BlockAssertions.createBooleansBlock;
import static com.facebook.presto.block.BlockAssertions.createDoublesBlock;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.operator.aggregation.AggregationTestUtils.assertAggregation;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

public class TestAreaUnderROCCurveAggregation
{
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    @Test
    public void testSingleClass()
    {
        testAggregation(null, createBooleansBlock(true, true, true), createDoublesBlock(0.3, 0.1, 0.2));
        testAggregation(null, createBooleansBlock(false, false, false), createDoublesBlock(0.3, 0.1, 0.2));
    }

    @Test
    public void testTrivial()
    {
        // max
        testAggregation(1.0, createBooleansBlock(true, true, false), createDoublesBlock(0.3, 0.2, 0.1));

        // min
        testAggregation(0.0, createBooleansBlock(false, false, true), createDoublesBlock(0.3, 0.2, 0.1));

        // mid
        testAggregation(0.5, createBooleansBlock(true, false, true), createDoublesBlock(0.3, 0.2, 0.1));
    }

    @Test
    public void testAUC()
    {
        // In Python scikit-learn machine learning library:
        // >>> from sklearn.metrics import roc_auc_score
        // >>> roc_auc_score([0, 1, 0, 1, 1], [0.5, 0.3, 0.2, 0.8, 0.7])
        // 0.83333333333333326
        testAggregation(0.8333333333, createBooleansBlock(false, true, false, true, true), createDoublesBlock(0.5, 0.3, 0.2, 0.8, 0.7));
    }

    private static void testAggregation(Double expectedValue, Block... blocks)
    {
        InternalAggregationFunction function = METADATA.getFunctionRegistry().getAggregateFunctionImplementation(
                new Signature("area_under_roc_curve",
                        AGGREGATE,
                        DOUBLE.getTypeSignature(),
                        BOOLEAN.getTypeSignature(),
                        DOUBLE.getTypeSignature()));
        assertAggregation(function, expectedValue, blocks);
    }
}
