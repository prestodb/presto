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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;

import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestAnyValueAggregation
        extends TestArbitraryAggregation
{
    private static final MetadataManager metadata = MetadataManager.createTestMetadataManager();
    private static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = metadata.getFunctionAndTypeManager();

    @Override
    protected JavaAggregationFunctionImplementation getAggregation(Type... arguments)
    {
        return FUNCTION_AND_TYPE_MANAGER.getJavaAggregateFunctionImplementation(FUNCTION_AND_TYPE_MANAGER.lookupFunction("any_value", fromTypes(arguments)));
    }
}
