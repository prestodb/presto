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
package com.facebook.presto.sql.relational;

import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.type.LikePatternType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.metadata.Signature.internalScalarFunction;
import static com.facebook.presto.spi.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.sql.tree.ArrayConstructor.ARRAY_CONSTRUCTOR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;

public final class Signatures
{
    public static final String IF = "IF";
    public static final String NULL_IF = "NULL_IF";
    public static final String SWITCH = "SWITCH";
    public static final String CAST = mangleOperatorName("CAST");
    public static final String TRY_CAST = "TRY_CAST";
    public static final String IS_NULL = "IS_NULL";
    public static final String COALESCE = "COALESCE";
    public static final String IN = "IN";
    public static final String TRY = "TRY";
    public static final String DEREFERENCE = "DEREFERENCE";
    public static final String ROW_CONSTRUCTOR = "ROW_CONSTRUCTOR";
    public static final String BIND = "$INTERNAL$BIND";

    private Signatures()
    {
    }

    // **************** sql operators ****************
    public static Signature notSignature()
    {
        return new Signature("not", SCALAR, parseTypeSignature(StandardTypes.BOOLEAN), ImmutableList.of(parseTypeSignature(StandardTypes.BOOLEAN)));
    }

    public static Signature betweenSignature(Type valueType, Type minType, Type maxType)
    {
        return internalOperator("BETWEEN", parseTypeSignature(StandardTypes.BOOLEAN), valueType.getTypeSignature(), minType.getTypeSignature(), maxType.getTypeSignature());
    }

    public static Signature likeVarcharSignature()
    {
        return internalScalarFunction("LIKE", parseTypeSignature(StandardTypes.BOOLEAN), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(LikePatternType.NAME));
    }

    public static Signature likeCharSignature(Type valueType)
    {
        checkArgument(valueType instanceof CharType, "Expected CHAR value type");
        return internalScalarFunction("LIKE", parseTypeSignature(StandardTypes.BOOLEAN), valueType.getTypeSignature(), parseTypeSignature(LikePatternType.NAME));
    }

    public static Signature likePatternSignature()
    {
        return internalScalarFunction("LIKE_PATTERN", parseTypeSignature(LikePatternType.NAME), parseTypeSignature(StandardTypes.VARCHAR), parseTypeSignature(StandardTypes.VARCHAR));
    }

    public static Signature castSignature(Type returnType, Type valueType)
    {
        // Name has already been mangled, so don't use internalOperator
        return internalScalarFunction(CAST, returnType.getTypeSignature(), valueType.getTypeSignature());
    }

    public static Signature tryCastSignature(Type returnType, Type valueType)
    {
        return internalScalarFunction(TRY_CAST, returnType.getTypeSignature(), valueType.getTypeSignature());
    }

    public static Signature logicalExpressionSignature(LogicalBinaryExpression.Operator operator)
    {
        return internalScalarFunction(operator.name(), parseTypeSignature(StandardTypes.BOOLEAN), parseTypeSignature(StandardTypes.BOOLEAN), parseTypeSignature(StandardTypes.BOOLEAN));
    }

    public static Signature arithmeticNegationSignature(Type returnType, Type valueType)
    {
        return internalOperator("NEGATION", returnType.getTypeSignature(), valueType.getTypeSignature());
    }

    public static Signature arithmeticExpressionSignature(ArithmeticBinaryExpression.Operator operator, Type returnType, Type leftType, Type rightType)
    {
        return internalOperator(operator.name(), returnType.getTypeSignature(), leftType.getTypeSignature(), rightType.getTypeSignature());
    }

    public static Signature subscriptSignature(Type returnType, Type leftType, Type rightType)
    {
        return internalOperator(SUBSCRIPT.name(), returnType.getTypeSignature(), leftType.getTypeSignature(), rightType.getTypeSignature());
    }

    public static Signature arrayConstructorSignature(Type returnType, List<? extends Type> argumentTypes)
    {
        return internalScalarFunction(ARRAY_CONSTRUCTOR, returnType.getTypeSignature(), Lists.transform(argumentTypes, Type::getTypeSignature));
    }

    public static Signature arrayConstructorSignature(TypeSignature returnType, List<TypeSignature> argumentTypes)
    {
        return internalScalarFunction(ARRAY_CONSTRUCTOR, returnType, argumentTypes);
    }

    public static Signature comparisonExpressionSignature(ComparisonExpression.Operator operator, Type leftType, Type rightType)
    {
        for (OperatorType operatorType : OperatorType.values()) {
            if (operatorType.name().equals(operator.name())) {
                return internalOperator(operator.name(), parseTypeSignature(StandardTypes.BOOLEAN), leftType.getTypeSignature(), rightType.getTypeSignature());
            }
        }
        return internalScalarFunction(operator.name(), parseTypeSignature(StandardTypes.BOOLEAN), leftType.getTypeSignature(), rightType.getTypeSignature());
    }

    // **************** special forms (lazy evaluation, etc) ****************
    public static Signature ifSignature(Type returnType)
    {
        return new Signature(IF, SCALAR, returnType.getTypeSignature());
    }

    public static Signature nullIfSignature(Type returnType, Type firstType, Type secondType)
    {
        return new Signature(NULL_IF, SCALAR, returnType.getTypeSignature(), firstType.getTypeSignature(), secondType.getTypeSignature());
    }

    public static Signature switchSignature(Type returnType)
    {
        return new Signature(SWITCH, SCALAR, returnType.getTypeSignature());
    }

    public static Signature whenSignature(Type returnType)
    {
        return new Signature("WHEN", SCALAR, returnType.getTypeSignature());
    }

    public static Signature trySignature(Type returnType)
    {
        return new Signature(TRY, SCALAR, returnType.getTypeSignature());
    }

    public static Signature bindSignature(Type returnType, List<Type> valueTypes, Type functionType)
    {
        ImmutableList.Builder<TypeSignature> typeSignatureBuilder = ImmutableList.builder();
        for (Type valueType : valueTypes) {
            typeSignatureBuilder.add(valueType.getTypeSignature());
        }
        typeSignatureBuilder.add(functionType.getTypeSignature());
        return new Signature(BIND, SCALAR, returnType.getTypeSignature(), typeSignatureBuilder.build());
    }

    // **************** functions that require varargs and/or complex types (e.g., lists) ****************
    public static Signature inSignature()
    {
        return internalScalarFunction(IN, parseTypeSignature(StandardTypes.BOOLEAN));
    }

    public static Signature rowConstructorSignature(Type returnType, List<Type> argumentTypes)
    {
        return internalScalarFunction(ROW_CONSTRUCTOR, returnType.getTypeSignature(), argumentTypes.stream().map(Type::getTypeSignature).collect(toImmutableList()));
    }

    // **************** functions that need to do special null handling ****************
    public static Signature isNullSignature(Type argumentType)
    {
        return internalScalarFunction(IS_NULL, parseTypeSignature(StandardTypes.BOOLEAN), argumentType.getTypeSignature());
    }

    public static Signature coalesceSignature(Type returnType, List<Type> argumentTypes)
    {
        return internalScalarFunction(COALESCE, returnType.getTypeSignature(), Lists.transform(argumentTypes, Type::getTypeSignature));
    }

    public static Signature dereferenceSignature(Type returnType, RowType rowType)
    {
        return internalScalarFunction(DEREFERENCE, returnType.getTypeSignature(), ImmutableList.of(rowType.getTypeSignature(), BigintType.BIGINT.getTypeSignature()));
    }
}
