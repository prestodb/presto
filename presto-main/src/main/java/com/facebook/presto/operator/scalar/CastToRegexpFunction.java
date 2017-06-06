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
import com.facebook.presto.metadata.SqlOperator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.analyzer.RegexLibrary;
import com.facebook.presto.type.JoniRegexp;
import com.facebook.presto.type.Re2JRegexp;
import com.facebook.presto.type.Regexp;
import com.facebook.presto.type.RegexpType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.OperatorType.CAST;
import static com.facebook.presto.spi.type.Chars.padSpaces;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.util.Reflection.methodHandle;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.util.Collections.emptyList;

public class CastToRegexpFunction
        extends SqlOperator
{
    private static final MethodHandle JONI_METHOD_HANDLE = methodHandle(CastToRegexpFunction.class, "castToJoniRegexp", boolean.class, long.class, Slice.class);
    private static final MethodHandle RE2J_METHOD_HANDLE = methodHandle(CastToRegexpFunction.class, "castToRe2JRegexp", int.class, int.class, boolean.class, long.class, Slice.class);

    private final int dfaStatesLimit;
    private final int dfaRetries;
    private final boolean padSpaces;
    private final RegexLibrary library;

    public static SqlOperator castVarcharToRegexp(RegexLibrary library, int dfaStatesLimit, int dfaRetries)
    {
        return new CastToRegexpFunction("varchar(x)", library, dfaStatesLimit, dfaRetries, false);
    }

    public static SqlOperator castCharToRegexp(RegexLibrary library, int dfaStatesLimit, int dfaRetries)
    {
        return new CastToRegexpFunction("char(x)", library, dfaStatesLimit, dfaRetries, true);
    }

    private CastToRegexpFunction(String sourceType, RegexLibrary library, int dfaStatesLimit, int dfaRetries, boolean padSpaces)
    {
        super(CAST, emptyList(), emptyList(), parseTypeSignature(RegexpType.NAME), ImmutableList.of(parseTypeSignature(sourceType, ImmutableSet.of("x"))));
        this.library = library;
        this.dfaStatesLimit = dfaStatesLimit;
        this.dfaRetries = dfaRetries;
        this.padSpaces = padSpaces;
    }

    @Override
    public ScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
    {
        switch (library) {
            case JONI:
                return new ScalarFunctionImplementation(
                        false,
                        ImmutableList.of(false),
                        insertArguments(JONI_METHOD_HANDLE, 0, padSpaces, boundVariables.getLongVariable("x")),
                        true);
            case RE2J:
                return new ScalarFunctionImplementation(
                        false,
                        ImmutableList.of(false),
                        insertArguments(RE2J_METHOD_HANDLE, 0, dfaStatesLimit, dfaRetries, padSpaces, boundVariables.getLongVariable("x")),
                        true);
            default:
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid regular expression library");
        }
    }

    public static Regexp castToJoniRegexp(boolean padSpaces, long typeLength, Slice pattern)
    {
        if (padSpaces) {
            pattern = padSpaces(pattern, (int) typeLength);
        }
        try {
            // When normal UTF8 encoding instead of non-strict UTF8) is used, joni can infinite loop when invalid UTF8 slice is supplied to it.
            return new JoniRegexp(pattern);
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }

    public static Regexp castToRe2JRegexp(int dfaStatesLimit, int dfaRetries, boolean padSpaces, long typeLength, Slice pattern)
    {
        if (padSpaces) {
            pattern = padSpaces(pattern, (int) typeLength);
        }
        try {
            return new Re2JRegexp(dfaStatesLimit, dfaRetries, pattern);
        }
        catch (Exception e) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, e);
        }
    }
}
