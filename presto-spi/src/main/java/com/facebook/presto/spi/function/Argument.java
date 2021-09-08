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
package com.facebook.presto.spi.function;

import javax.annotation.Nullable;

import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.spi.function.Argument.ArgumentType.FUNCTION_TYPE;
import static com.facebook.presto.spi.function.Argument.ArgumentType.VALUE_TYPE;
import static java.lang.String.format;

public class Argument
{
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

        // Add checkArgument, checkState functions to remove dep from guava
        public static void checkArgument(boolean expression, @Nullable Object errorMessage)
        {
            if (!expression) {
                throw new IllegalArgumentException(String.valueOf(errorMessage));
            }
        }

        public static void checkState(boolean expression, @Nullable Object errorMessage)
        {
            if (!expression) {
                throw new IllegalStateException(String.valueOf(errorMessage));
            }
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
}
