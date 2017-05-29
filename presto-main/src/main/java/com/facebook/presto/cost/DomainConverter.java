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

package com.facebook.presto.cost;

import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import io.airlift.slice.Slice;

import java.util.OptionalDouble;

import static java.util.Collections.singletonList;

/**
 * This will contain set of function used in process of calculation stats.
 * It is mostly for mapping Type domain to double domain which is used for range comparisons
 * during stats computations.
 */
public class DomainConverter
{
    private final Type type;
    private final FunctionRegistry functionRegistry;
    private final ConnectorSession session;

    public DomainConverter(Type type, FunctionRegistry functionRegistry, ConnectorSession session)
    {
        this.type = type;
        this.functionRegistry = functionRegistry;
        this.session = session;
    }

    public Slice castToVarchar(Object object)
    {
        Signature castSignature = functionRegistry.getCoercion(type, VarcharType.createUnboundedVarcharType());
        ScalarFunctionImplementation castImplementation = functionRegistry.getScalarFunctionImplementation(castSignature);
        return (Slice) ExpressionInterpreter.invoke(session, castImplementation, singletonList(object));
    }

    public OptionalDouble translateToDouble(Object object)
    {
        if (!isDoubleTranslationSupported(type)) {
            return OptionalDouble.empty();
        }
        Signature castSignature = functionRegistry.getCoercion(type, DoubleType.DOUBLE);
        ScalarFunctionImplementation castImplementation = functionRegistry.getScalarFunctionImplementation(castSignature);
        return OptionalDouble.of((double) ExpressionInterpreter.invoke(session, castImplementation, singletonList(object)));
    }

    private boolean isDoubleTranslationSupported(Type type)
    {
        return type instanceof DecimalType
                || DoubleType.DOUBLE.equals(type)
                || RealType.REAL.equals(type)
                || BigintType.BIGINT.equals(type)
                || IntegerType.INTEGER.equals(type)
                || SmallintType.SMALLINT.equals(type)
                || TinyintType.TINYINT.equals(type)
                || BooleanType.BOOLEAN.equals(type);
    }
}
