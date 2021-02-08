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
package com.facebook.presto.operator.scalar.annotations;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.UnknownType;
import com.google.common.base.Joiner;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;

public final class OperatorValidator
{
    private OperatorValidator() {}

    public static void validateOperator(OperatorType operatorType, TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        switch (operatorType) {
            case ADD:
            case SUBTRACT:
            case MULTIPLY:
            case DIVIDE:
            case MODULUS:
                validateOperatorSignature(operatorType, returnType, argumentTypes, 2);
                break;
            case NEGATION:
                validateOperatorSignature(operatorType, returnType, argumentTypes, 1);
                break;
            case EQUAL:
            case NOT_EQUAL:
            case LESS_THAN:
            case LESS_THAN_OR_EQUAL:
            case GREATER_THAN:
            case GREATER_THAN_OR_EQUAL:
                validateComparisonOperatorSignature(operatorType, returnType, argumentTypes, 2);
                break;
            case BETWEEN:
                validateComparisonOperatorSignature(operatorType, returnType, argumentTypes, 3);
                break;
            case CAST:
                validateOperatorSignature(operatorType, returnType, argumentTypes, 1);
                break;
            case SUBSCRIPT:
                validateOperatorSignature(operatorType, returnType, argumentTypes, 2);
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
                break;
            case HASH_CODE:
                validateOperatorSignature(operatorType, returnType, argumentTypes, 1);
                checkArgument(returnType.getBase().equals(StandardTypes.BIGINT), "%s operator must return a BIGINT: %s", operatorType, formatSignature(operatorType, returnType, argumentTypes));
                break;
            case SATURATED_FLOOR_CAST:
                validateOperatorSignature(operatorType, returnType, argumentTypes, 1);
                break;
        }
    }

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
