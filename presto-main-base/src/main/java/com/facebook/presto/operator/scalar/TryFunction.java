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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.function.TypeParameterSpecialization;
import com.facebook.presto.sql.gen.lambda.LambdaFunctionInterface;
import io.airlift.slice.Slice;

import java.util.function.Supplier;

import static com.facebook.presto.spi.StandardErrorCode.DIVISION_BY_ZERO;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_CAST_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.StandardErrorCode.NUMERIC_VALUE_OUT_OF_RANGE;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;

@Description("internal try function for desugaring TRY")
@ScalarFunction(value = "$internal$try", visibility = HIDDEN, deterministic = false)
public final class TryFunction
{
    private TryFunction() {}

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = long.class)
    @SqlNullable
    @SqlType("T")
    public static Long tryLong(@SqlType("function(T)") TryLongLambda function)
    {
        try {
            return function.apply();
        }
        catch (PrestoException e) {
            propagateIfUnhandled(e);
            return null;
        }
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = double.class)
    @SqlNullable
    @SqlType("T")
    public static Double tryDouble(@SqlType("function(T)") TryDoubleLambda function)
    {
        try {
            return function.apply();
        }
        catch (PrestoException e) {
            propagateIfUnhandled(e);
            return null;
        }
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = boolean.class)
    @SqlNullable
    @SqlType("T")
    public static Boolean tryBoolean(@SqlType("function(T)") TryBooleanLambda function)
    {
        try {
            return function.apply();
        }
        catch (PrestoException e) {
            propagateIfUnhandled(e);
            return null;
        }
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Slice.class)
    @SqlNullable
    @SqlType("T")
    public static Slice trySlice(@SqlType("function(T)") TrySliceLambda function)
    {
        try {
            return function.apply();
        }
        catch (PrestoException e) {
            propagateIfUnhandled(e);
            return null;
        }
    }

    @TypeParameter("T")
    @TypeParameterSpecialization(name = "T", nativeContainerType = Block.class)
    @SqlNullable
    @SqlType("T")
    public static Block tryBlock(@SqlType("function(T)") TryBlockLambda function)
    {
        try {
            return function.apply();
        }
        catch (PrestoException e) {
            propagateIfUnhandled(e);
            return null;
        }
    }

    @FunctionalInterface
    public interface TryLongLambda
            extends LambdaFunctionInterface
    {
        Long apply();
    }

    @FunctionalInterface
    public interface TryDoubleLambda
            extends LambdaFunctionInterface
    {
        Double apply();
    }

    @FunctionalInterface
    public interface TryBooleanLambda
            extends LambdaFunctionInterface
    {
        Boolean apply();
    }

    @FunctionalInterface
    public interface TrySliceLambda
            extends LambdaFunctionInterface
    {
        Slice apply();
    }

    @FunctionalInterface
    public interface TryBlockLambda
            extends LambdaFunctionInterface
    {
        Block apply();
    }

    public static <T> T evaluate(Supplier<T> supplier, T defaultValue)
    {
        try {
            return supplier.get();
        }
        catch (PrestoException e) {
            propagateIfUnhandled(e);
            return defaultValue;
        }
    }

    private static void propagateIfUnhandled(PrestoException e)
            throws PrestoException
    {
        int errorCode = e.getErrorCode().getCode();
        if (errorCode == DIVISION_BY_ZERO.toErrorCode().getCode()
                || errorCode == INVALID_CAST_ARGUMENT.toErrorCode().getCode()
                || errorCode == INVALID_FUNCTION_ARGUMENT.toErrorCode().getCode()
                || errorCode == NUMERIC_VALUE_OUT_OF_RANGE.toErrorCode().getCode()) {
            return;
        }

        throw e;
    }
}
