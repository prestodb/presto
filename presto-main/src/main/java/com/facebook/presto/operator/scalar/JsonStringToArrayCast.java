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

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.Signature.typeVariable;
import static com.facebook.presto.operator.scalar.JsonToArrayCast.JSON_TO_ARRAY;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

public final class JsonStringToArrayCast
        extends SqlScalarFunction
{
    public static final JsonStringToArrayCast JSON_STRING_TO_ARRAY = new JsonStringToArrayCast();
    public static final String JSON_STRING_TO_ARRAY_NAME = "$internal$json_string_to_array_cast";

    private JsonStringToArrayCast()
    {
        super(new Signature(
                JSON_STRING_TO_ARRAY_NAME,
                SCALAR,
                ImmutableList.of(typeVariable("T")),
                ImmutableList.of(),
                parseTypeSignature("array(T)"),
                ImmutableList.of(parseTypeSignature(StandardTypes.VARCHAR)),
                false));
    }

    @Override
    public String getDescription()
    {
        // Internal function, doesn't need a description
        return null;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public final boolean isHidden()
    {
        return true;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        return JSON_TO_ARRAY.specialize(boundVariables, arity, typeManager, functionRegistry);
    }
}
