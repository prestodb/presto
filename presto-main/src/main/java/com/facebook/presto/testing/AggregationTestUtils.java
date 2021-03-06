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
package com.facebook.presto.testing;

import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.aggregation.AggregationFromAnnotationsParser;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.google.common.annotations.VisibleForTesting;

import java.util.List;

import static com.facebook.presto.metadata.FunctionAndTypeManager.createTestFunctionAndTypeManager;

public class AggregationTestUtils
{
    private AggregationTestUtils()
    {
    }

    @VisibleForTesting
    public static InternalAggregationFunction generateInternalAggregationFunction(Class<?> clazz, TypeSignature outputType, List<TypeSignature> inputTypes)
    {
        return generateInternalAggregationFunction(clazz, outputType, inputTypes, createTestFunctionAndTypeManager());
    }

    @VisibleForTesting
    public static InternalAggregationFunction generateInternalAggregationFunction(Class<?> clazz, TypeSignature outputType, List<TypeSignature> inputTypes, FunctionAndTypeManager functionAndTypeManager)
    {
        return generateInternalAggregationFunction(clazz, outputType, inputTypes, functionAndTypeManager, BoundVariables.builder().build(), inputTypes.size());
    }

    @VisibleForTesting
    public static InternalAggregationFunction generateInternalAggregationFunction(Class<?> clazz, TypeSignature outputType, List<TypeSignature> inputTypes, FunctionAndTypeManager functionAndTypeManager, BoundVariables boundVariables, int arity)
    {
        return AggregationFromAnnotationsParser.parseFunctionDefinitionWithTypesConstraint(clazz, outputType, inputTypes).specialize(boundVariables, arity, functionAndTypeManager);
    }
}
