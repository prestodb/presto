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
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.type.Re2JRegexp;
import com.facebook.presto.type.Re2JRegexpType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.common.function.OperatorType.CAST;
import static com.facebook.presto.common.type.Chars.padSpaces;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.util.Collections.emptyList;

public class Re2JCastToRegexpFunction
        extends SqlOperator
{
    private static final MethodHandle METHOD_HANDLE = methodHandle(Re2JCastToRegexpFunction.class, "castToRegexp", int.class, int.class, boolean.class, long.class, Slice.class);

    private final int dfaStatesLimit;
    private final int dfaRetries;
    private final boolean padSpaces;

    public static SqlOperator castVarcharToRe2JRegexp(int dfaStatesLimit, int dfaRetries)
    {
        return new Re2JCastToRegexpFunction("varchar(x)", dfaStatesLimit, dfaRetries, false);
    }

    public static SqlOperator castCharToRe2JRegexp(int dfaStatesLimit, int dfaRetries)
    {
        return new Re2JCastToRegexpFunction("char(x)", dfaStatesLimit, dfaRetries, true);
    }

    private Re2JCastToRegexpFunction(String sourceType, int dfaStatesLimit, int dfaRetries, boolean padSpaces)
    {
        super(CAST, emptyList(), emptyList(), parseTypeSignature(Re2JRegexpType.NAME), ImmutableList.of(parseTypeSignature(sourceType, ImmutableSet.of("x"))));
        this.dfaStatesLimit = dfaStatesLimit;
        this.dfaRetries = dfaRetries;
        this.padSpaces = padSpaces;
    }

    @Override
    public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
    {
        return new BuiltInScalarFunctionImplementation(
                false,
                ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                insertArguments(METHOD_HANDLE, 0, dfaStatesLimit, dfaRetries, padSpaces, boundVariables.getLongVariable("x")));
    }

    public static Re2JRegexp castToRegexp(int dfaStatesLimit, int dfaRetries, boolean padSpaces, long typeLength, Slice pattern)
    {
        try {
            if (padSpaces) {
                pattern = padSpaces(pattern, (int) typeLength);
            }
            return new Re2JRegexp(dfaStatesLimit, dfaRetries, pattern);
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }
}
