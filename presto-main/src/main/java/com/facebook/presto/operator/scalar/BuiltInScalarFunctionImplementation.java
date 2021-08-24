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

import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentType.FUNCTION_TYPE;
import static com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation.ArgumentType.VALUE_TYPE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public final class BuiltInScalarFunctionImplementation
        implements ScalarFunctionImplementation
{
    private final List<ScalarImplementationChoice> choices;

    public BuiltInScalarFunctionImplementation(
            boolean nullable,
            List<ArgumentProperty> argumentProperties,
            MethodHandle methodHandle)
    {
        this(
                nullable,
                argumentProperties,
                methodHandle,
                Optional.empty());
    }

    public BuiltInScalarFunctionImplementation(
            boolean nullable,
            List<ArgumentProperty> argumentProperties,
            MethodHandle methodHandle,
            Optional<MethodHandle> instanceFactory)
    {
        this(
                ImmutableList.of(new ScalarImplementationChoice(
                        nullable,
                        argumentProperties,
                        ReturnPlaceConvention.STACK,
                        methodHandle,
                        instanceFactory)));
    }

    /**
     * Creates a ScalarFunctionImplementation consisting of one or more choices.
     * <p>
     * All choices must have the same SQL signature, and are equivalent in what they do.
     * The first choice is the default choice, which is the one used for legacy access methods.
     * The default choice must be usable under any context. (e.g. it must not use BLOCK_POSITION convention.)
     *
     * @param choices the list of choices, ordered from generic to specific
     */
    public BuiltInScalarFunctionImplementation(List<ScalarImplementationChoice> choices)
    {
        checkArgument(!choices.isEmpty(), "choices is an empty list");
        this.choices = ImmutableList.copyOf(choices);
    }

    public boolean isNullable()
    {
        return choices.get(0).isNullable();
    }

    public ArgumentProperty getArgumentProperty(int argumentIndex)
    {
        return choices.get(0).argumentProperties.get(argumentIndex);
    }

    public MethodHandle getMethodHandle()
    {
        return choices.get(0).methodHandle;
    }

    public Optional<MethodHandle> getInstanceFactory()
    {
        return choices.get(0).instanceFactory;
    }

    public List<ScalarImplementationChoice> getAllChoices()
    {
        return choices;
    }

    public static class ScalarImplementationChoice
    {
        private final boolean nullable;
        private final List<ArgumentProperty> argumentProperties;
        private final ReturnPlaceConvention returnPlaceConvention;
        private final MethodHandle methodHandle;
        private final Optional<MethodHandle> instanceFactory;
        private final boolean hasProperties;

        public ScalarImplementationChoice(
                boolean nullable,
                List<ArgumentProperty> argumentProperties,
                ReturnPlaceConvention returnPlaceConvention,
                MethodHandle methodHandle,
                Optional<MethodHandle> instanceFactory)
        {
            this.nullable = nullable;
            this.argumentProperties = ImmutableList.copyOf(requireNonNull(argumentProperties, "argumentProperties is null"));
            this.returnPlaceConvention = requireNonNull(returnPlaceConvention, "returnPlaceConvention is null");
            this.methodHandle = requireNonNull(methodHandle, "methodHandle is null");
            this.instanceFactory = requireNonNull(instanceFactory, "instanceFactory is null");

            if (instanceFactory.isPresent()) {
                Class<?> instanceType = instanceFactory.get().type().returnType();
                checkArgument(instanceFactory.get().type().parameterList().size() == 0, "instanceFactory should have no parameter");
                checkArgument(instanceType.equals(methodHandle.type().parameterType(0)), "methodHandle is not an instance method");
            }

            List<Class<?>> parameterList = methodHandle.type().parameterList();
            boolean hasProperties = false;
            if (parameterList.contains(SqlFunctionProperties.class)) {
                checkArgument(parameterList.stream().filter(SqlFunctionProperties.class::equals).count() == 1, "function implementation should have exactly one SqlFunctionProperties parameter");
                if (!instanceFactory.isPresent()) {
                    checkArgument(parameterList.get(0) == SqlFunctionProperties.class, "SqlFunctionProperties must be the first argument when instanceFactory is not present");
                }
                else {
                    checkArgument(parameterList.get(1) == SqlFunctionProperties.class, "SqlFunctionProperties must be the second argument when instanceFactory is present");
                }
                hasProperties = true;
            }
            this.hasProperties = hasProperties;
        }

        public boolean isNullable()
        {
            return nullable;
        }

        public List<ArgumentProperty> getArgumentProperties()
        {
            return argumentProperties;
        }

        public ArgumentProperty getArgumentProperty(int argumentIndex)
        {
            return argumentProperties.get(argumentIndex);
        }

        public ReturnPlaceConvention getReturnPlaceConvention()
        {
            return returnPlaceConvention;
        }

        public MethodHandle getMethodHandle()
        {
            return methodHandle;
        }

        public Optional<MethodHandle> getInstanceFactory()
        {
            return instanceFactory;
        }

        public boolean hasProperties()
        {
            return hasProperties;
        }
    }

    public static class ArgumentProperty
    {
        // TODO: Alternatively, we can store com.facebook.presto.spi.type.Type
        private final ArgumentType argumentType;
        private final Optional<NullConvention> nullConvention;
        private final Optional<Class> lambdaInterface;

        public static ArgumentProperty valueTypeArgumentProperty(NullConvention nullConvention)
        {
            return new ArgumentProperty(VALUE_TYPE, Optional.of(nullConvention), Optional.empty());
        }

        public static ArgumentProperty functionTypeArgumentProperty(Class lambdaInterface)
        {
            return new ArgumentProperty(FUNCTION_TYPE, Optional.empty(), Optional.of(lambdaInterface));
        }

        public ArgumentProperty(ArgumentType argumentType, Optional<NullConvention> nullConvention, Optional<Class> lambdaInterface)
        {
            switch (argumentType) {
                case VALUE_TYPE:
                    checkArgument(nullConvention.isPresent(), "nullConvention must present for value type");
                    checkArgument(!lambdaInterface.isPresent(), "lambdaInterface must not present for value type");
                    break;
                case FUNCTION_TYPE:
                    checkArgument(!nullConvention.isPresent(), "nullConvention must not present for function type");
                    checkArgument(lambdaInterface.isPresent(), "lambdaInterface must present for function type");
                    checkArgument(lambdaInterface.get().isAnnotationPresent(FunctionalInterface.class), "lambdaInterface must be annotated with FunctionalInterface");
                    break;
                default:
                    throw new UnsupportedOperationException(format("Unsupported argument type: %s", argumentType));
            }

            this.argumentType = argumentType;
            this.nullConvention = nullConvention;
            this.lambdaInterface = lambdaInterface;
        }

        public ArgumentType getArgumentType()
        {
            return argumentType;
        }

        public NullConvention getNullConvention()
        {
            checkState(getArgumentType() == VALUE_TYPE, "nullConvention only applies to value type argument");
            return nullConvention.get();
        }

        public Class getLambdaInterface()
        {
            checkState(getArgumentType() == FUNCTION_TYPE, "lambdaInterface only applies to function type argument");
            return lambdaInterface.get();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }

            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }

            ArgumentProperty other = (ArgumentProperty) obj;
            return this.argumentType == other.argumentType &&
                    this.nullConvention.equals(other.nullConvention) &&
                    this.lambdaInterface.equals(other.lambdaInterface);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(nullConvention, lambdaInterface);
        }
    }

    public enum NullConvention
    {
        RETURN_NULL_ON_NULL(1),
        USE_BOXED_TYPE(1),
        USE_NULL_FLAG(2),
        BLOCK_AND_POSITION(2),
        /**/;

        private final int parameterCount;

        NullConvention(int parameterCount)
        {
            this.parameterCount = parameterCount;
        }

        public int getParameterCount()
        {
            return parameterCount;
        }
    }

    public enum ArgumentType
    {
        VALUE_TYPE,
        FUNCTION_TYPE
    }

    public enum ReturnPlaceConvention
    {
        STACK,
        PROVIDED_BLOCKBUILDER
    }
}
