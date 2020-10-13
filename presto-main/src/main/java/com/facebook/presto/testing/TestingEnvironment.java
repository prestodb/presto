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

import com.facebook.presto.block.BlockEncodingManager;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.type.TypeRegistry;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;

public class TestingEnvironment
{
    private TestingEnvironment() {}

    public static final TypeManager TYPE_MANAGER = new TypeRegistry();
    public static final FunctionAndTypeManager FUNCTION_AND_TYPE_MANAGER = new FunctionAndTypeManager(TYPE_MANAGER, new BlockEncodingManager(), new FeaturesConfig());

    public static MethodHandle getOperatorMethodHandle(OperatorType operatorType, Type... parameterTypes)
    {
        return FUNCTION_AND_TYPE_MANAGER.getBuiltInScalarFunctionImplementation(FUNCTION_AND_TYPE_MANAGER.resolveOperator(operatorType, fromTypes(parameterTypes))).getMethodHandle();
    }
}
