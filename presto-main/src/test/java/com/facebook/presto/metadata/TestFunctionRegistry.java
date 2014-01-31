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
package com.facebook.presto.metadata;

import com.facebook.presto.operator.scalar.CustomAdd;
import com.facebook.presto.operator.scalar.ScalarFunction;
import org.testng.annotations.Test;

import java.util.List;

public class TestFunctionRegistry
{
    @Test(expectedExceptions = IllegalArgumentException.class, expectedExceptionsMessageRegExp = "\\QFunction already registered: custom_add(bigint,bigint):bigint\\E")
    public void testDuplicateFunctions()
    {
        List<FunctionInfo> functions = new FunctionRegistry.FunctionListBuilder()
                .scalar(CustomAdd.class)
                .build();

        FunctionRegistry registry = new FunctionRegistry();
        registry.addFunctions(functions);
        registry.addFunctions(functions);
    }

    @Test(expectedExceptions = IllegalStateException.class, expectedExceptionsMessageRegExp = "'sum' is both an aggregation and a scalar function")
    public void testConflictingScalarAggregation()
            throws Exception
    {
        List<FunctionInfo> functions = new FunctionRegistry.FunctionListBuilder()
                .scalar(ScalarSum.class)
                .build();

        FunctionRegistry registry = new FunctionRegistry();
        registry.addFunctions(functions);
    }

    public static final class ScalarSum
    {
        private ScalarSum() {}

        @ScalarFunction
        public static long sum(long a, long b)
        {
            return a + b;
        }
    }
}
