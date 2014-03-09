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
package com.facebook.presto.metadata;

import com.facebook.presto.sql.gen.FunctionBinder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.NullType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public final class OperatorInfo
{
    public static enum OperatorType
    {
        ADD("+")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateOperatorSignature(this, returnType, argumentTypes, 2);
                    }
                },

        SUBTRACT("-")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateOperatorSignature(this, returnType, argumentTypes, 2);
                    }
                },

        MULTIPLY("*")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateOperatorSignature(this, returnType, argumentTypes, 2);
                    }
                },

        DIVIDE("/")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateOperatorSignature(this, returnType, argumentTypes, 2);
                    }
                },

        MODULUS("%")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateOperatorSignature(this, returnType, argumentTypes, 2);
                    }
                },

        NEGATION("-")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateOperatorSignature(this, returnType, argumentTypes, 1);
                    }
                },

        EQUAL("=")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateComparisonOperatorSignature(this, returnType, argumentTypes, 2);
                    }
                },

        NOT_EQUAL("<>")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateComparisonOperatorSignature(this, returnType, argumentTypes, 2);
                    }
                },

        LESS_THAN("<")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateComparisonOperatorSignature(this, returnType, argumentTypes, 2);
                    }
                },
        LESS_THAN_OR_EQUAL("<=")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateComparisonOperatorSignature(this, returnType, argumentTypes, 2);
                    }
                },

        GREATER_THAN(">")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateComparisonOperatorSignature(this, returnType, argumentTypes, 2);
                    }
                },

        GREATER_THAN_OR_EQUAL(">=")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateComparisonOperatorSignature(this, returnType, argumentTypes, 2);
                    }
                },

        BETWEEN("BETWEEN")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateComparisonOperatorSignature(this, returnType, argumentTypes, 3);
                    }
                },

        CAST("CAST")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateOperatorSignature(this, returnType, argumentTypes, 1);
                    }
                },

        HASH_CODE("HASH CODE")
                {
                    @Override
                    void validateSignature(Type returnType, List<Type> argumentTypes)
                    {
                        validateOperatorSignature(this, returnType, argumentTypes, 1);
                        checkArgument(returnType.equals(BigintType.BIGINT), "%s operator must return a BIGINT: %s", this, formatSignature(this, returnType, argumentTypes));
                    }
                };

        private final String operator;

        OperatorType(String operator)
        {
            this.operator = operator;
        }

        public String getOperator()
        {
            return operator;
        }

        abstract void validateSignature(Type returnType, List<Type> argumentTypes);

        private static void validateOperatorSignature(OperatorType operatorType, Type returnType, List<Type> argumentTypes, int expectedArgumentCount)
        {
            String signature = formatSignature(operatorType, returnType, argumentTypes);
            checkArgument(!returnType.equals(NullType.NULL), "%s operator return type can not be NULL: %s", operatorType, signature);
            checkArgument(argumentTypes.size() == expectedArgumentCount, "%s operator must have exactly %s argument: %s", operatorType, expectedArgumentCount, signature);
        }

        private static void validateComparisonOperatorSignature(OperatorType operatorType, Type returnType, List<Type> argumentTypes, int expectedArgumentCount)
        {
            validateOperatorSignature(operatorType, returnType, argumentTypes, expectedArgumentCount);
            checkArgument(returnType.equals(BooleanType.BOOLEAN), "%s operator must return a BOOLEAN: %s", operatorType, formatSignature(operatorType, returnType, argumentTypes));
        }
    }

    private final OperatorType operatorType;
    private final Type returnType;
    private final List<Type> argumentTypes;

    private final MethodHandle methodHandle;
    private final FunctionBinder functionBinder;

    public OperatorInfo(OperatorType operatorType, Type returnType, List<Type> argumentTypes, MethodHandle function, FunctionBinder functionBinder)
    {
        this.operatorType = checkNotNull(operatorType, "operator is null");
        this.returnType = checkNotNull(returnType, "returnType is null");
        this.argumentTypes = ImmutableList.copyOf(checkNotNull(argumentTypes, "argumentTypes is null"));
        this.functionBinder = checkNotNull(functionBinder, "functionBinder is null");
        this.methodHandle = checkNotNull(function, "function is null");

        operatorType.validateSignature(returnType, argumentTypes);
    }

    public OperatorType getOperatorType()
    {
        return operatorType;
    }

    public Type getReturnType()
    {
        return returnType;
    }

    public List<Type> getArgumentTypes()
    {
        return argumentTypes;
    }

    public MethodHandle getMethodHandle()
    {
        return methodHandle;
    }

    public FunctionBinder getFunctionBinder()
    {
        return functionBinder;
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(operatorType, returnType, argumentTypes);
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
        OperatorInfo other = (OperatorInfo) obj;
        return Objects.equal(this.operatorType, other.operatorType) &&
                Objects.equal(this.returnType, other.returnType) &&
                Objects.equal(this.argumentTypes, other.argumentTypes);
    }

    @Override
    public String toString()
    {
        return formatSignature(operatorType, returnType, argumentTypes);
    }

    private static String formatSignature(OperatorType operatorType, Type returnType, List<Type> argumentTypes)
    {
        return operatorType + "(" + Joiner.on(", ").join(argumentTypes) + ")::" + returnType;
    }

    public static Function<OperatorInfo, OperatorType> operatorGetter()
    {
        return new Function<OperatorInfo, OperatorType>()
        {
            @Override
            public OperatorType apply(OperatorInfo input)
            {
                return input.getOperatorType();
            }
        };
    }
}
