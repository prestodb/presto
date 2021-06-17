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
package com.facebook.presto.common.function;

import com.facebook.presto.common.ErrorCodeSupplier;
import com.facebook.presto.common.PrestoException;
import com.facebook.presto.common.StandardErrorCode;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention;
import com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention;
import com.facebook.presto.common.type.Type;
import io.airlift.slice.Slice;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;

import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.BLOCK_POSITION;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.BOXED_NULLABLE;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.FUNCTION;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.NEVER_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationArgumentConvention.NULL_FLAG;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.FAIL_ON_NULL;
import static com.facebook.presto.common.function.InvocationConvention.InvocationReturnConvention.NULLABLE_RETURN;
import static com.facebook.presto.common.function.ScalarFunctionAdapter.NullAdaptationPolicy.RETURN_NULL_ON_NULL;
import static com.facebook.presto.common.function.ScalarFunctionAdapter.NullAdaptationPolicy.THROW_ON_NULL;
import static com.facebook.presto.common.function.ScalarFunctionAdapter.NullAdaptationPolicy.UNDEFINED_VALUE_FOR_NULL;
import static com.facebook.presto.common.function.ScalarFunctionAdapter.NullAdaptationPolicy.UNSUPPORTED;
import static java.lang.invoke.MethodHandles.collectArguments;
import static java.lang.invoke.MethodHandles.constant;
import static java.lang.invoke.MethodHandles.dropArguments;
import static java.lang.invoke.MethodHandles.explicitCastArguments;
import static java.lang.invoke.MethodHandles.filterArguments;
import static java.lang.invoke.MethodHandles.filterReturnValue;
import static java.lang.invoke.MethodHandles.guardWithTest;
import static java.lang.invoke.MethodHandles.identity;
import static java.lang.invoke.MethodHandles.insertArguments;
import static java.lang.invoke.MethodHandles.lookup;
import static java.lang.invoke.MethodHandles.permuteArguments;
import static java.lang.invoke.MethodHandles.publicLookup;
import static java.lang.invoke.MethodHandles.throwException;
import static java.lang.invoke.MethodType.methodType;
import static java.util.Objects.requireNonNull;

public class ScalarFunctionAdapter
{
    private static final MethodHandle IS_NULL_METHOD = lookupIsNullMethod();

    private final NullAdaptationPolicy nullAdaptationPolicy;

    public ScalarFunctionAdapter(NullAdaptationPolicy nullAdaptationPolicy)
    {
        this.nullAdaptationPolicy = requireNonNull(nullAdaptationPolicy, "nullAdaptationPolicy is null");
    }

    /**
     * Can the actual calling convention of a method be converted to the expected calling convention?
     */
    public boolean canAdapt(InvocationConvention actualConvention, InvocationConvention expectedConvention)
    {
        requireNonNull(actualConvention, "actualConvention is null");
        requireNonNull(expectedConvention, "expectedConvention is null");
        if (actualConvention.getArgumentConventions().size() != expectedConvention.getArgumentConventions().size()) {
            throw new IllegalArgumentException("Actual and expected conventions have different number of arguments");
        }

        if (actualConvention.supportsSession() && !expectedConvention.supportsSession()) {
            return false;
        }

        if (actualConvention.supportsInstanceFactor() && !expectedConvention.supportsInstanceFactor()) {
            return false;
        }

        if (!canAdaptReturn(actualConvention.getReturnConvention(), expectedConvention.getReturnConvention())) {
            return false;
        }

        for (int argumentIndex = 0; argumentIndex < actualConvention.getArgumentConventions().size(); argumentIndex++) {
            InvocationArgumentConvention actualArgumentConvention = actualConvention.getArgumentConvention(argumentIndex);
            InvocationArgumentConvention expectedArgumentConvention = expectedConvention.getArgumentConvention(argumentIndex);
            if (!canAdaptParameter(
                    actualArgumentConvention,
                    expectedArgumentConvention,
                    expectedConvention.getReturnConvention())) {
                return false;
            }
        }
        return true;
    }

    private boolean canAdaptReturn(
            InvocationReturnConvention actualReturnConvention,
            InvocationReturnConvention expectedReturnConvention)
    {
        if (actualReturnConvention == expectedReturnConvention) {
            return true;
        }

        if (expectedReturnConvention == NULLABLE_RETURN && actualReturnConvention == FAIL_ON_NULL) {
            return true;
        }

        if (expectedReturnConvention == FAIL_ON_NULL && actualReturnConvention == NULLABLE_RETURN) {
            switch (nullAdaptationPolicy) {
                case THROW_ON_NULL:
                case UNDEFINED_VALUE_FOR_NULL:
                    return true;
                case UNSUPPORTED:
                case RETURN_NULL_ON_NULL:
                    return false;
                default:
                    return false;
            }
        }

        return false;
    }

    private boolean canAdaptParameter(
            InvocationArgumentConvention actualArgumentConvention,
            InvocationArgumentConvention expectedArgumentConvention,
            InvocationReturnConvention returnConvention)
    {
        // not a conversion
        if (actualArgumentConvention == expectedArgumentConvention) {
            return true;
        }

        // no conversions from function or block and position are supported
        if (actualArgumentConvention == BLOCK_POSITION || actualArgumentConvention == FUNCTION) {
            return false;
        }

        // caller will never pass null, so all conversions are allowed
        if (expectedArgumentConvention == NEVER_NULL) {
            return true;
        }

        // nulls are passed in blocks, so adapter will handle null or throw exception at runtime
        if (expectedArgumentConvention == BLOCK_POSITION) {
            return true;
        }

        // null is passed as boxed value or a boolean null flag
        if (expectedArgumentConvention == BOXED_NULLABLE || expectedArgumentConvention == NULL_FLAG) {
            // null able to not nullable has special handling
            if (actualArgumentConvention == NEVER_NULL) {
                switch (nullAdaptationPolicy) {
                    case THROW_ON_NULL:
                    case UNDEFINED_VALUE_FOR_NULL:
                        return true;
                    case RETURN_NULL_ON_NULL:
                        return returnConvention != FAIL_ON_NULL;
                    case UNSUPPORTED:
                        return false;
                    default:
                        return false;
                }
            }

            return true;
        }

        return false;
    }

    /**
     * Adapt the method handle from the actual calling convention of a method be converted to the expected calling convention?
     */
    public MethodHandle adapt(
            MethodHandle methodHandle,
            List<Type> actualArgumentTypes,
            InvocationConvention actualConvention,
            InvocationConvention expectedConvention)
    {
        requireNonNull(methodHandle, "methodHandle is null");
        requireNonNull(actualConvention, "actualConvention is null");
        requireNonNull(expectedConvention, "expectedConvention is null");
        if (expectedConvention.getArgumentConventions().size() != expectedConvention.getArgumentConventions().size()) {
            throw new IllegalArgumentException("Actual and expected conventions have different number of arguments");
        }

        if (actualConvention.supportsSession() && !expectedConvention.supportsSession()) {
            throw new IllegalArgumentException("Session method can not be adapted to no session");
        }
        if (!(expectedConvention.supportsInstanceFactor() || !actualConvention.supportsInstanceFactor())) {
            throw new IllegalArgumentException("Instance method can not be adapted to no instance");
        }

        // adapt return first, since return-null-on-null parameter convention must know if the return type is nullable
        methodHandle = adaptReturn(methodHandle, actualConvention.getReturnConvention(), expectedConvention.getReturnConvention());

        // adapt parameters one at a time
        int parameterIndex = 0;

        if (actualConvention.supportsInstanceFactor()) {
            parameterIndex++;
        }
        if (actualConvention.supportsSession()) {
            parameterIndex++;
        }

        for (int argumentIndex = 0; argumentIndex < actualConvention.getArgumentConventions().size(); argumentIndex++) {
            Type argumentType = actualArgumentTypes.get(argumentIndex);
            InvocationArgumentConvention actualArgumentConvention = actualConvention.getArgumentConvention(argumentIndex);
            InvocationArgumentConvention expectedArgumentConvention = expectedConvention.getArgumentConvention(argumentIndex);
            methodHandle = adaptParameter(
                    methodHandle,
                    parameterIndex,
                    argumentType,
                    actualArgumentConvention,
                    expectedArgumentConvention,
                    expectedConvention.getReturnConvention());
            parameterIndex++;
            if (expectedArgumentConvention == NULL_FLAG || expectedArgumentConvention == BLOCK_POSITION) {
                parameterIndex++;
            }
        }
        return methodHandle;
    }

    private MethodHandle adaptReturn(
            MethodHandle methodHandle,
            InvocationReturnConvention actualReturnConvention,
            InvocationReturnConvention expectedReturnConvention)
    {
        if (actualReturnConvention == expectedReturnConvention) {
            return methodHandle;
        }

        Class<?> returnType = methodHandle.type().returnType();
        if (expectedReturnConvention == NULLABLE_RETURN) {
            if (actualReturnConvention == FAIL_ON_NULL) {
                // box return
                return explicitCastArguments(methodHandle, methodHandle.type().changeReturnType(wrap(returnType)));
            }
        }

        if (expectedReturnConvention == FAIL_ON_NULL) {
            if (actualReturnConvention == NULLABLE_RETURN) {
                if (nullAdaptationPolicy == UNSUPPORTED || nullAdaptationPolicy == RETURN_NULL_ON_NULL) {
                    throw new IllegalArgumentException("Nullable return can not be adapted fail on null");
                }

                if (nullAdaptationPolicy == UNDEFINED_VALUE_FOR_NULL) {
                    // currently, we just perform unboxing, which converts nulls to Java primitive default value
                    methodHandle = explicitCastArguments(methodHandle, methodHandle.type().changeReturnType(unwrap(returnType)));
                    return methodHandle;
                }

                if (nullAdaptationPolicy == THROW_ON_NULL) {
                    MethodHandle adapter = identity(returnType);
                    adapter = explicitCastArguments(adapter, adapter.type().changeReturnType(unwrap(returnType)));
                    adapter = guardWithTest(
                            isNullArgument(adapter.type(), 0),
                            throwPrestoNullArgumentException(adapter.type()),
                            adapter);

                    return filterReturnValue(methodHandle, adapter);
                }
            }
        }
        throw new IllegalArgumentException("Unsupported return convention: " + actualReturnConvention);
    }

    private MethodHandle adaptParameter(
            MethodHandle methodHandle,
            int parameterIndex,
            Type argumentType,
            InvocationArgumentConvention actualArgumentConvention,
            InvocationArgumentConvention expectedArgumentConvention,
            InvocationReturnConvention returnConvention)
    {
        if (actualArgumentConvention == expectedArgumentConvention) {
            return methodHandle;
        }
        if (actualArgumentConvention == BLOCK_POSITION) {
            throw new IllegalArgumentException("Block and position argument can not be adapted");
        }
        if (actualArgumentConvention == FUNCTION) {
            throw new IllegalArgumentException("Function argument can not be adapted");
        }

        // caller will never pass null
        if (expectedArgumentConvention == NEVER_NULL) {
            if (actualArgumentConvention == BOXED_NULLABLE) {
                // if actual argument is boxed primitive, change method handle to accept a primitive and then box to actual method
                if (isWrapperType(methodHandle.type().parameterType(parameterIndex))) {
                    MethodType targetType = methodHandle.type().changeParameterType(parameterIndex, unwrap(methodHandle.type().parameterType(parameterIndex)));
                    methodHandle = explicitCastArguments(methodHandle, targetType);
                }
                return methodHandle;
            }

            if (actualArgumentConvention == NULL_FLAG) {
                // actual method takes value and null flag, so change method handle to not have the flag and always pass false to the actual method
                return insertArguments(methodHandle, parameterIndex + 1, false);
            }

            throw new IllegalArgumentException("Unsupported actual argument convention: " + actualArgumentConvention);
        }

        // caller will pass Java null for SQL null
        if (expectedArgumentConvention == BOXED_NULLABLE) {
            if (actualArgumentConvention == NEVER_NULL) {
                if (nullAdaptationPolicy == UNSUPPORTED) {
                    throw new IllegalArgumentException("Not null argument can not be adapted to nullable");
                }

                // box argument
                Class<?> boxedType = wrap(methodHandle.type().parameterType(parameterIndex));
                MethodType targetType = methodHandle.type().changeParameterType(parameterIndex, boxedType);
                methodHandle = explicitCastArguments(methodHandle, targetType);

                if (nullAdaptationPolicy == UNDEFINED_VALUE_FOR_NULL) {
                    // currently, we just perform unboxing, which converts nulls to Java primitive default value
                    return methodHandle;
                }

                if (nullAdaptationPolicy == RETURN_NULL_ON_NULL) {
                    if (returnConvention == FAIL_ON_NULL) {
                        throw new IllegalArgumentException("RETURN_NULL_ON_NULL adaptation can not be used with FAIL_ON_NULL return convention");
                    }
                    return guardWithTest(
                            isNullArgument(methodHandle.type(), parameterIndex),
                            returnNull(methodHandle.type()),
                            methodHandle);
                }

                if (nullAdaptationPolicy == THROW_ON_NULL) {
                    MethodType adapterType = methodType(boxedType, boxedType);
                    MethodHandle adapter = guardWithTest(
                            isNullArgument(adapterType, 0),
                            throwPrestoNullArgumentException(adapterType),
                            identity(boxedType));

                    return collectArguments(methodHandle, parameterIndex, adapter);
                }
            }

            if (actualArgumentConvention == NULL_FLAG) {
                // The conversion is described below in reverse order as this is how method handle adaptation works.  The provided example
                // signature is based on a a boxed Long argument.

                // 3. unbox the value (if null the java default is sent)
                // long, boolean => Long, boolean
                Class<?> parameterType = methodHandle.type().parameterType(parameterIndex);
                methodHandle = explicitCastArguments(methodHandle, methodHandle.type().changeParameterType(parameterIndex, wrap(parameterType)));

                // 2. replace second argument with the result of isNull
                // long, boolean => Long, Long
                methodHandle = filterArguments(
                        methodHandle,
                        parameterIndex + 1,
                        explicitCastArguments(IS_NULL_METHOD, methodType(boolean.class, wrap(parameterType))));

                // 1. Duplicate the argument, so we have two copies of the value
                // Long, Long => Long
                int[] reorder = IntStream.range(0, methodHandle.type().parameterCount())
                        .map(i -> i <= parameterIndex ? i : i - 1)
                        .toArray();
                MethodType newType = methodHandle.type().dropParameterTypes(parameterIndex + 1, parameterIndex + 2);
                methodHandle = permuteArguments(methodHandle, newType, reorder);
                return methodHandle;
            }

            throw new IllegalArgumentException("Unsupported actual argument convention: " + actualArgumentConvention);
        }

        // caller will pass boolean true in the next argument for SQL null
        if (expectedArgumentConvention == NULL_FLAG) {
            if (actualArgumentConvention == NEVER_NULL) {
                if (nullAdaptationPolicy == UNSUPPORTED) {
                    throw new IllegalArgumentException("Not null argument can not be adapted to nullable");
                }

                if (nullAdaptationPolicy == UNDEFINED_VALUE_FOR_NULL) {
                    // add null flag to call
                    methodHandle = dropArguments(methodHandle, parameterIndex + 1, boolean.class);
                    return methodHandle;
                }

                // if caller sets null flag, return null, otherwise invoke target
                if (nullAdaptationPolicy == RETURN_NULL_ON_NULL) {
                    if (returnConvention == FAIL_ON_NULL) {
                        throw new IllegalArgumentException("RETURN_NULL_ON_NULL adaptation can not be used with FAIL_ON_NULL return convention");
                    }
                    // add null flag to call
                    methodHandle = dropArguments(methodHandle, parameterIndex + 1, boolean.class);
                    return guardWithTest(
                            isTrueNullFlag(methodHandle.type(), parameterIndex),
                            returnNull(methodHandle.type()),
                            methodHandle);
                }

                if (nullAdaptationPolicy == THROW_ON_NULL) {
                    MethodHandle adapter = identity(methodHandle.type().parameterType(parameterIndex));
                    adapter = dropArguments(adapter, 1, boolean.class);
                    adapter = guardWithTest(
                            isTrueNullFlag(adapter.type(), 0),
                            throwPrestoNullArgumentException(adapter.type()),
                            adapter);

                    return collectArguments(methodHandle, parameterIndex, adapter);
                }
            }

            if (actualArgumentConvention == BOXED_NULLABLE) {
                return collectArguments(methodHandle, parameterIndex, boxedToNullFlagFilter(methodHandle.type().parameterType(parameterIndex)));
            }

            throw new IllegalArgumentException("Unsupported actual argument convention: " + actualArgumentConvention);
        }

        // caller will pass boolean true in the next argument for SQL null
        if (expectedArgumentConvention == BLOCK_POSITION) {
            MethodHandle getBlockValue = getBlockValue(argumentType, methodHandle.type().parameterType(parameterIndex));

            if (actualArgumentConvention == NEVER_NULL) {
                if (nullAdaptationPolicy == UNDEFINED_VALUE_FOR_NULL) {
                    // Current, null is not checked, so whatever type returned is passed through
                    methodHandle = collectArguments(methodHandle, parameterIndex, getBlockValue);
                    return methodHandle;
                }

                if (nullAdaptationPolicy == RETURN_NULL_ON_NULL && returnConvention != FAIL_ON_NULL) {
                    // if caller sets null flag, return null, otherwise invoke target
                    methodHandle = collectArguments(methodHandle, parameterIndex, getBlockValue);
                    return guardWithTest(
                            isBlockPositionNull(methodHandle.type(), parameterIndex),
                            returnNull(methodHandle.type()),
                            methodHandle);
                }

                if (nullAdaptationPolicy == THROW_ON_NULL || nullAdaptationPolicy == UNSUPPORTED || nullAdaptationPolicy == RETURN_NULL_ON_NULL) {
                    MethodHandle adapter = guardWithTest(
                            isBlockPositionNull(getBlockValue.type(), 0),
                            throwPrestoNullArgumentException(getBlockValue.type()),
                            getBlockValue);

                    return collectArguments(methodHandle, parameterIndex, adapter);
                }
            }

            if (actualArgumentConvention == BOXED_NULLABLE) {
                getBlockValue = explicitCastArguments(getBlockValue, getBlockValue.type().changeReturnType(wrap(getBlockValue.type().returnType())));
                getBlockValue = guardWithTest(
                        isBlockPositionNull(getBlockValue.type(), 0),
                        returnNull(getBlockValue.type()),
                        getBlockValue);
                methodHandle = collectArguments(methodHandle, parameterIndex, getBlockValue);
                return methodHandle;
            }

            if (actualArgumentConvention == NULL_FLAG) {
                // long, boolean => long, Block, int
                MethodHandle isNull = isBlockPositionNull(getBlockValue.type(), 0);
                methodHandle = collectArguments(methodHandle, parameterIndex + 1, isNull);

                // long, Block, int => Block, int, Block, int
                getBlockValue = guardWithTest(
                        isBlockPositionNull(getBlockValue.type(), 0),
                        returnNull(getBlockValue.type()),
                        getBlockValue);
                methodHandle = collectArguments(methodHandle, parameterIndex, getBlockValue);

                int[] reorder = IntStream.range(0, methodHandle.type().parameterCount())
                        .map(i -> i <= parameterIndex + 1 ? i : i - 2)
                        .toArray();
                MethodType newType = methodHandle.type().dropParameterTypes(parameterIndex + 2, parameterIndex + 4);
                methodHandle = permuteArguments(methodHandle, newType, reorder);
                return methodHandle;
            }

            throw new IllegalArgumentException("Unsupported actual argument convention: " + actualArgumentConvention);
        }

        throw new IllegalArgumentException("Unsupported expected argument convention: " + expectedArgumentConvention);
    }

    private static MethodHandle getBlockValue(Type argumentType, Class<?> expectedType)
    {
        Class<?> methodArgumentType = argumentType.getJavaType();
        String getterName;
        if (methodArgumentType == boolean.class) {
            getterName = "getBoolean";
        }
        else if (methodArgumentType == long.class) {
            getterName = "getLong";
        }
        else if (methodArgumentType == double.class) {
            getterName = "getDouble";
        }
        else if (methodArgumentType == Slice.class) {
            getterName = "getSlice";
        }
        else {
            getterName = "getObject";
            methodArgumentType = Object.class;
        }

        try {
            MethodHandle getValue = lookup().findVirtual(Type.class, getterName, methodType(methodArgumentType, Block.class, int.class))
                    .bindTo(argumentType);
            return explicitCastArguments(getValue, getValue.type().changeReturnType(expectedType));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static MethodHandle boxedToNullFlagFilter(Class<?> argumentType)
    {
        // Start with identity
        MethodHandle handle = identity(argumentType);
        // if argument is a primitive, box it
        if (isWrapperType(argumentType)) {
            handle = explicitCastArguments(handle, handle.type().changeParameterType(0, unwrap(argumentType)));
        }
        // Add boolean null flag
        handle = dropArguments(handle, 1, boolean.class);
        // if flag is true, return null, otherwise invoke identity
        return guardWithTest(
                isTrueNullFlag(handle.type(), 0),
                returnNull(handle.type()),
                handle);
    }

    private static MethodHandle isTrueNullFlag(MethodType methodType, int index)
    {
        return permuteArguments(identity(boolean.class), methodType.changeReturnType(boolean.class), index + 1);
    }

    private static MethodHandle isNullArgument(MethodType methodType, int index)
    {
        // Start with Objects.isNull(Object):boolean
        MethodHandle isNull = IS_NULL_METHOD;
        // Cast in incoming type: isNull(T):boolean
        isNull = explicitCastArguments(isNull, methodType(boolean.class, methodType.parameterType(index)));
        // Add extra argument to match expected method type
        isNull = permuteArguments(isNull, methodType.changeReturnType(boolean.class), index);
        return isNull;
    }

    private static MethodHandle isBlockPositionNull(MethodType methodType, int index)
    {
        // Start with Objects.isNull(Object):boolean
        MethodHandle isNull;
        try {
            isNull = lookup().findVirtual(Block.class, "isNull", methodType(boolean.class, int.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
        // Add extra argument to match expected method type
        isNull = permuteArguments(isNull, methodType.changeReturnType(boolean.class), index, index + 1);
        return isNull;
    }

    private static MethodHandle lookupIsNullMethod()
    {
        MethodHandle isNull;
        try {
            isNull = lookup().findStatic(Objects.class, "isNull", methodType(boolean.class, Object.class));
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
        return isNull;
    }

    private static MethodHandle returnNull(MethodType methodType)
    {
        // Start with a constant null value of the expected return type: f():R
        MethodHandle returnNull = constant(wrap(methodType.returnType()), null);

        // Add extra argument to match expected method type: f(a, b, c, ..., n):R
        returnNull = permuteArguments(returnNull, methodType.changeReturnType(wrap(methodType.returnType())));

        // Convert return to a primitive is necessary: f(a, b, c, ..., n):r
        returnNull = explicitCastArguments(returnNull, methodType);
        return returnNull;
    }

    private static MethodHandle throwPrestoNullArgumentException(MethodType type)
    {
        MethodHandle throwException = collectArguments(throwException(type.returnType(), PrestoException.class), 0, prestoNullArgumentException());
        return permuteArguments(throwException, type);
    }

    private static MethodHandle prestoNullArgumentException()
    {
        try {
            return publicLookup().findConstructor(PrestoException.class, methodType(void.class, ErrorCodeSupplier.class, String.class))
                    .bindTo(StandardErrorCode.INVALID_FUNCTION_ARGUMENT)
                    .bindTo("A never null argument is null");
        }
        catch (ReflectiveOperationException e) {
            throw new AssertionError(e);
        }
    }

    private static boolean isWrapperType(Class<?> type)
    {
        return type != unwrap(type);
    }

    private static Class<?> wrap(Class<?> type)
    {
        return methodType(type).wrap().returnType();
    }

    private static Class<?> unwrap(Class<?> type)
    {
        return methodType(type).unwrap().returnType();
    }

    public enum NullAdaptationPolicy
    {
        UNSUPPORTED,
        THROW_ON_NULL,
        RETURN_NULL_ON_NULL,
        UNDEFINED_VALUE_FOR_NULL
    }
}
