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
package com.facebook.presto.sql.relational;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.common.function.OperatorType.ADD;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestFunctionResolution
{
    private FunctionResolution functionResolution;

    @BeforeClass
    public void setup()
    {
        FunctionAndTypeManager functionAndTypeManager = createTestFunctionAndTypeManager();
        functionResolution = new FunctionResolution(functionAndTypeManager);
    }

    @Test
    public void testStandardFunctionResolution()
    {
        StandardFunctionResolution standardFunctionResolution = functionResolution;

        // not
        assertTrue(standardFunctionResolution.isNotFunction(standardFunctionResolution.notFunction()));

        // negate
        assertTrue(standardFunctionResolution.isNegateFunction(standardFunctionResolution.negateFunction(DOUBLE)));
        assertFalse(standardFunctionResolution.isNotFunction(standardFunctionResolution.negateFunction(DOUBLE)));

        // arithmetic
        assertTrue(standardFunctionResolution.isArithmeticFunction(standardFunctionResolution.arithmeticFunction(ADD, DOUBLE, DOUBLE)));
        assertFalse(standardFunctionResolution.isComparisonFunction(standardFunctionResolution.arithmeticFunction(ADD, DOUBLE, DOUBLE)));

        // comparison
        assertTrue(standardFunctionResolution.isComparisonFunction(standardFunctionResolution.comparisonFunction(GREATER_THAN, DOUBLE, DOUBLE)));
        assertFalse(standardFunctionResolution.isArithmeticFunction(standardFunctionResolution.comparisonFunction(GREATER_THAN, DOUBLE, DOUBLE)));

        // between
        assertTrue(standardFunctionResolution.isBetweenFunction(standardFunctionResolution.betweenFunction(DOUBLE, DOUBLE, DOUBLE)));
        assertFalse(standardFunctionResolution.isNotFunction(standardFunctionResolution.betweenFunction(DOUBLE, DOUBLE, DOUBLE)));

        // subscript
        assertTrue(standardFunctionResolution.isSubscriptFunction(standardFunctionResolution.subscriptFunction(new ArrayType(DOUBLE), BIGINT)));
        assertFalse(standardFunctionResolution.isBetweenFunction(standardFunctionResolution.subscriptFunction(new ArrayType(DOUBLE), BIGINT)));
    }
}
