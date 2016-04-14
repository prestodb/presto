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

import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.type.UnknownType;
import com.google.common.base.Joiner;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public enum OperatorType
{
    ADD("+")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateOperatorSignature(this, returnType, argumentTypes, 2);
                }
            },

    SUBTRACT("-")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateOperatorSignature(this, returnType, argumentTypes, 2);
                }
            },

    MULTIPLY("*")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateOperatorSignature(this, returnType, argumentTypes, 2);
                }
            },

    DIVIDE("/")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateOperatorSignature(this, returnType, argumentTypes, 2);
                }
            },

    MODULUS("%")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateOperatorSignature(this, returnType, argumentTypes, 2);
                }
            },

    NEGATION("-")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateOperatorSignature(this, returnType, argumentTypes, 1);
                }
            },

    EQUAL("=")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateComparisonOperatorSignature(this, returnType, argumentTypes, 2);
                }
            },

    NOT_EQUAL("<>")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateComparisonOperatorSignature(this, returnType, argumentTypes, 2);
                }
            },

    LESS_THAN("<")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateComparisonOperatorSignature(this, returnType, argumentTypes, 2);
                }
            },
    LESS_THAN_OR_EQUAL("<=")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateComparisonOperatorSignature(this, returnType, argumentTypes, 2);
                }
            },

    GREATER_THAN(">")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateComparisonOperatorSignature(this, returnType, argumentTypes, 2);
                }
            },

    GREATER_THAN_OR_EQUAL(">=")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateComparisonOperatorSignature(this, returnType, argumentTypes, 2);
                }
            },

    BETWEEN("BETWEEN")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateComparisonOperatorSignature(this, returnType, argumentTypes, 3);
                }
            },

    CAST("CAST")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateOperatorSignature(this, returnType, argumentTypes, 1);
                }
            },

    SUBSCRIPT("[]")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateOperatorSignature(this, returnType, argumentTypes, 2);
                    checkArgument(argumentTypes.get(0).getBase().equals(StandardTypes.ARRAY) || argumentTypes.get(0).getBase().equals(StandardTypes.MAP), "First argument must be an ARRAY or MAP");
                    if (argumentTypes.get(0).getBase().equals(StandardTypes.ARRAY)) {
                        checkArgument(argumentTypes.get(1).getBase().equals(StandardTypes.BIGINT), "Second argument must be a BIGINT");
                        TypeSignature elementType = argumentTypes.get(0).getTypeParametersAsTypeSignatures().get(0);
                        checkArgument(returnType.equals(elementType), "[] return type does not match ARRAY element type");
                    }
                    else {
                        TypeSignature valueType = argumentTypes.get(0).getTypeParametersAsTypeSignatures().get(1);
                        checkArgument(returnType.equals(valueType), "[] return type does not match MAP value type");
                    }
                }
            },

    HASH_CODE("HASH CODE")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateOperatorSignature(this, returnType, argumentTypes, 1);
                    checkArgument(returnType.getBase().equals(StandardTypes.BIGINT), "%s operator must return a BIGINT: %s", this, formatSignature(this, returnType, argumentTypes));
                }
            },

    SATURATED_FLOOR_CAST("SATURATED FLOOR CAST")
            {
                @Override
                void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
                {
                    validateOperatorSignature(this, returnType, argumentTypes, 1);
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

    abstract void validateSignature(TypeSignature returnType, List<TypeSignature> argumentTypes);

    private static void validateOperatorSignature(OperatorType operatorType, TypeSignature returnType, List<TypeSignature> argumentTypes, int expectedArgumentCount)
    {
        String signature = formatSignature(operatorType, returnType, argumentTypes);
        checkArgument(!returnType.getBase().equals(UnknownType.NAME), "%s operator return type can not be NULL: %s", operatorType, signature);
        checkArgument(argumentTypes.size() == expectedArgumentCount, "%s operator must have exactly %s argument: %s", operatorType, expectedArgumentCount, signature);
    }

    private static void validateComparisonOperatorSignature(OperatorType operatorType, TypeSignature returnType, List<TypeSignature> argumentTypes, int expectedArgumentCount)
    {
        validateOperatorSignature(operatorType, returnType, argumentTypes, expectedArgumentCount);
        checkArgument(returnType.getBase().equals(StandardTypes.BOOLEAN), "%s operator must return a BOOLEAN: %s", operatorType, formatSignature(operatorType, returnType, argumentTypes));
    }

    private static String formatSignature(OperatorType operatorType, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return operatorType + "(" + Joiner.on(", ").join(argumentTypes) + ")::" + returnType;
    }
}
