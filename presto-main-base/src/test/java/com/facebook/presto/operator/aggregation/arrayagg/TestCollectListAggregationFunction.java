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
package com.facebook.presto.operator.aggregation.arrayagg;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.aggregation.BuiltInAggregationFunctionImplementation;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static com.facebook.presto.operator.aggregation.arrayagg.ArrayAggGroupImplementation.NEW;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotNull;

@Test(singleThreaded = true)
public class TestCollectListAggregationFunction
{
    private FunctionAndTypeManager functionAndTypeManager;

    @BeforeMethod
    public void setUp()
    {
        functionAndTypeManager = createTestFunctionAndTypeManager();
    }

    @Test
    public void testFunctionName()
    {
        CollectListAggregationFunction function = new CollectListAggregationFunction(false, NEW);
        assertEquals(function.getSignature().getName().getObjectName(), "collect_list");
    }

    @Test
    public void testDescription()
    {
        CollectListAggregationFunction function = new CollectListAggregationFunction(false, NEW);
        assertEquals(function.getDescription(), "return an array of values (alias for array_agg)");
    }

    @Test
    public void testSignature()
    {
        CollectListAggregationFunction function = new CollectListAggregationFunction(false, NEW);

        // Verify signature has correct structure
        assertEquals(function.getSignature().getTypeVariableConstraints().size(), 1);
        assertEquals(function.getSignature().getTypeVariableConstraints().get(0).getName(), "T");

        // Verify return type is array(T)
        assertEquals(function.getSignature().getReturnType().toString(), "array(T)");

        // Verify single argument of type T
        assertEquals(function.getSignature().getArgumentTypes().size(), 1);
        assertEquals(function.getSignature().getArgumentTypes().get(0).toString(), "T");
    }

    @Test
    public void testSpecializeWithBigint()
    {
        CollectListAggregationFunction function = new CollectListAggregationFunction(false, NEW);

        BoundVariables boundVariables = BoundVariables.builder()
                .setTypeVariable("T", BIGINT)
                .build();

        // Verify it doesn't throw an exception and returns a valid implementation
        BuiltInAggregationFunctionImplementation implementation = function.specialize(boundVariables, 1, functionAndTypeManager);
        assertNotNull(implementation);
    }

    @Test
    public void testSpecializeWithVarchar()
    {
        CollectListAggregationFunction function = new CollectListAggregationFunction(false, NEW);

        BoundVariables boundVariables = BoundVariables.builder()
                .setTypeVariable("T", VARCHAR)
                .build();

        // Verify it doesn't throw an exception and returns a valid implementation
        BuiltInAggregationFunctionImplementation implementation = function.specialize(boundVariables, 1, functionAndTypeManager);
        assertNotNull(implementation);
    }

    @Test
    public void testLegacyMode()
    {
        // Test with legacy mode enabled
        CollectListAggregationFunction legacyFunction = new CollectListAggregationFunction(true, NEW);

        BoundVariables boundVariables = BoundVariables.builder()
                .setTypeVariable("T", BIGINT)
                .build();

        BuiltInAggregationFunctionImplementation implementation = legacyFunction.specialize(boundVariables, 1, functionAndTypeManager);
        assertNotNull(implementation);
    }

    @Test
    public void testDifferentGroupImplementations()
    {
        // Test with different ArrayAggGroupImplementation values
        for (ArrayAggGroupImplementation groupImpl : ArrayAggGroupImplementation.values()) {
            CollectListAggregationFunction function = new CollectListAggregationFunction(false, groupImpl);

            BoundVariables boundVariables = BoundVariables.builder()
                    .setTypeVariable("T", BIGINT)
                    .build();

            BuiltInAggregationFunctionImplementation implementation = function.specialize(boundVariables, 1, functionAndTypeManager);
            assertNotNull(implementation);
        }
    }
}
