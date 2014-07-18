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
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.tree.ArithmeticExpression;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.LogicalBinaryExpression;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.facebook.presto.metadata.Signature.internalFunction;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;

public final class Signatures
{
    public static final String IF = "IF";
    public static final String NULL_IF = "NULL_IF";
    public static final String SWITCH = "SWITCH";
    public static final String CAST = "CAST";
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
        return new Signature("not", BOOLEAN, ImmutableList.of(BOOLEAN));
    }

    public static Signature betweenSignature(Type valueType, Type minType, Type maxType)
    {
        return internalFunction("BETWEEN", BOOLEAN, valueType, minType, maxType);
    }

    public static Signature likeSignature()
    {
        return internalFunction("LIKE", BOOLEAN, VARCHAR, LIKE_PATTERN);
    }

    public static Signature likePatternSignature()
    {
        return internalFunction("LIKE_PATTERN", LIKE_PATTERN, VARCHAR, VARCHAR);
    }

    public static Signature castSignature(Type returnType, Type valueType)
    {
        return internalFunction(CAST, returnType, valueType);
    }

    public static Signature tryCastSignature(Type returnType, Type valueType)
    {
        return internalFunction(TRY_CAST, returnType, valueType);
    }

    public static Signature logicalExpressionSignature(LogicalBinaryExpression.Type expressionType)
    {
        return internalFunction(expressionType.name(), BOOLEAN, BOOLEAN, BOOLEAN);
    }

    public static Signature arithmeticNegationSignature(Type returnType, Type valueType)
    {
        return internalFunction("NEGATION", returnType, valueType);
    }

    public static Signature arithmeticExpressionSignature(ArithmeticExpression.Type expressionType, Type returnType, Type leftType, Type rightType)
    {
        return internalFunction(expressionType.name(), returnType, leftType, rightType);
    }

    public static Signature comparisonExpressionSignature(ComparisonExpression.Type expressionType, Type leftType, Type rightType)
    {
        return internalFunction(expressionType.name(), BOOLEAN, leftType, rightType);
    }

    // **************** special forms (lazy evaluation, etc) ****************
    public static Signature ifSignature(Type returnType)
    {
        return new Signature(IF, returnType);
    }

    public static Signature nullIfSignature(Type returnType, Type firstType, Type secondType)
    {
        return new Signature(NULL_IF, returnType, firstType, secondType);
    }

    public static Signature switchSignature(Type returnType)
    {
        return new Signature(SWITCH, returnType);
    }

    public static Signature whenSignature(Type returnType)
    {
        return new Signature("WHEN", returnType);
    }

    // **************** functions that require varargs and/or complex types (e.g., lists) ****************
    public static Signature inSignature()
    {
        return internalFunction(IN, BOOLEAN);
    }

    // **************** functions that need to do special null handling ****************
    public static Signature isNullSignature(Type argumentType)
    {
        return internalFunction(IS_NULL, BOOLEAN, argumentType);
    }

    public static Signature coalesceSignature(Type returnType, List<Type> argumentTypes)
    {
        return internalFunction(COALESCE, returnType, argumentTypes);
    }
}
