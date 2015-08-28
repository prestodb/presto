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

import com.facebook.presto.client.FailureInfo;
import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricScalar;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.type.JsonType;
import com.google.common.collect.ImmutableList;
import io.airlift.json.JsonCodec;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.util.Map;

import static com.facebook.presto.util.Reflection.methodHandle;

public class FailureFunction
        extends ParametricScalar
{
    public static final FailureFunction FAILURE_FUNCTION = new FailureFunction();

    private static final JsonCodec<FailureInfo> JSON_CODEC = JsonCodec.jsonCodec(FailureInfo.class);
    private static final String FUNCTION_NAME = "fail";

    // TODO return type is unknown, fix this when we get rid on unknown type
    private static final Signature SIGNATURE = new Signature(FUNCTION_NAME, ImmutableList.of(), "UNKNOWN", ImmutableList.of(StandardTypes.JSON), false, true);

    @Override
    public Signature getSignature()
    {
        return SIGNATURE;
    }

    @Override
    public boolean isHidden()
    {
        return true;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public String getDescription()
    {
        return "Decodes json to an exception and throws it";
    }

    @Override
    public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        MethodHandle methodHandle = methodHandle(FailureFunction.class, "fail", Slice.class);
        Signature signature = new Signature(FUNCTION_NAME, "UNKNOWN", JsonType.JSON.toString());
        return new FunctionInfo(signature, getDescription(), isHidden(), methodHandle, isDeterministic(), false, ImmutableList.of(false));
    }

    public static Block fail(Slice json)
    {
        throw JSON_CODEC.fromJson(json.getBytes()).toException();
    }
}
