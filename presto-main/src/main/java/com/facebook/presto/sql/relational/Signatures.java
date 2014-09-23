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

import com.facebook.presto.metadata.OperatorType;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.facebook.presto.type.LikePatternType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.List;

import static com.facebook.presto.metadata.FunctionRegistry.mangleOperatorName;
import static com.facebook.presto.metadata.OperatorType.SUBSCRIPT;
import static com.facebook.presto.metadata.Signature.internalFunction;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.sql.tree.ArrayConstructor.ARRAY_CONSTRUCTOR;
import static com.facebook.presto.type.TypeUtils.nameGetter;

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

    private Signatures()
    {
    }

    // **************** sql operators ****************
    public static Signature notSignature()
    {
        return new Signature("not", StandardTypes.BOOLEAN, ImmutableList.of(StandardTypes.BOOLEAN));
    }

    public static Signature betweenSignature(Type valueType, Type minType, Type maxType)
    {
        return internalOperator("BETWEEN", StandardTypes.BOOLEAN, valueType.getName(), minType.getName(), maxType.getName());
    }

    public static Signature likeSignature()
    {
        return internalFunction("LIKE", StandardTypes.BOOLEAN, StandardTypes.VARCHAR, LikePatternType.NAME);
    }

    public static Signature likePatternSignature()
    {
        return internalFunction("LIKE_PATTERN", LikePatternType.NAME, StandardTypes.VARCHAR, StandardTypes.VARCHAR);
    }

    public static Signature castSignature(Type returnType, Type valueType)
    {
        // Name has already been mangled, so don't use internalOperator
        return internalFunction(CAST, returnType.getName(), valueType.getName());
    }

    public static Signature tryCastSignature(Type returnType, Type valueType)
    {
        return internalFunction(TRY_CAST, returnType.getName(), valueType.getName());
    }

    public static Signature logicalExpressionSignature(LogicalBinaryExpression.Type expressionType)
    {
        return internalFunction(expressionType.name(), StandardTypes.BOOLEAN, StandardTypes.BOOLEAN, StandardTypes.BOOLEAN);
    }

    public static Signature arithmeticNegationSignature(Type returnType, Type valueType)
    {
        return internalOperator("NEGATION", returnType.getName(), valueType.getName());
    }

    public static Signature arithmeticExpressionSignature(ArithmeticExpression.Type expressionType, Type returnType, Type leftType, Type rightType)
    {
        return internalOperator(expressionType.name(), returnType.getName(), leftType.getName(), rightType.getName());
    }

    public static Signature subscriptSignature(Type returnType, Type leftType, Type rightType)
    {
        return internalOperator(SUBSCRIPT.name(), returnType.getName(), leftType.getName(), rightType.getName());
    }

    public static Signature arrayConstructorSignature(Type returnType, List<? extends Type> argumentTypes)
    {
        return internalFunction(ARRAY_CONSTRUCTOR, returnType.getName(), Lists.transform(argumentTypes, nameGetter()));
    }

    public static Signature arrayConstructorSignature(String returnType, List<String> argumentTypes)
    {
        return internalFunction(ARRAY_CONSTRUCTOR, returnType, argumentTypes);
    }

    public static Signature comparisonExpressionSignature(ComparisonExpression.Type expressionType, Type leftType, Type rightType)
    {
        for (OperatorType operatorType : OperatorType.values()) {
            if (operatorType.name().equals(expressionType.name())) {
                return internalOperator(expressionType.name(), StandardTypes.BOOLEAN, leftType.getName(), rightType.getName());
            }
        }
        return internalFunction(expressionType.name(), StandardTypes.BOOLEAN, leftType.getName(), rightType.getName());
    }

    // **************** special forms (lazy evaluation, etc) ****************
    public static Signature ifSignature(Type returnType)
    {
        return new Signature(IF, returnType.getName());
    }

    public static Signature nullIfSignature(Type returnType, Type firstType, Type secondType)
    {
        return new Signature(NULL_IF, returnType.getName(), firstType.getName(), secondType.getName());
    }

    public static Signature switchSignature(Type returnType)
    {
        return new Signature(SWITCH, returnType.getName());
    }

    public static Signature whenSignature(Type returnType)
    {
        return new Signature("WHEN", returnType.getName());
    }

    // **************** functions that require varargs and/or complex types (e.g., lists) ****************
    public static Signature inSignature()
    {
        return internalFunction(IN, StandardTypes.BOOLEAN);
    }

    // **************** functions that need to do special null handling ****************
    public static Signature isNullSignature(Type argumentType)
    {
        return internalFunction(IS_NULL, StandardTypes.BOOLEAN, argumentType.getName());
    }

    public static Signature coalesceSignature(Type returnType, List<Type> argumentTypes)
    {
        return internalFunction(COALESCE, returnType.getName(), Lists.transform(argumentTypes, nameGetter()));
    }
}
