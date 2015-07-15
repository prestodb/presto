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
package com.facebook.presto.type;

import com.facebook.presto.metadata.FunctionInfo;
import com.facebook.presto.metadata.FunctionRegistry;
import com.facebook.presto.metadata.ParametricOperator;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.util.Reflection;
import com.google.common.collect.ImmutableList;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.Map;

import static com.facebook.presto.metadata.FunctionRegistry.operatorInfo;
import static com.facebook.presto.metadata.OperatorType.ADD;
import static com.facebook.presto.metadata.Signature.comparableWithVariadicBound;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.lang.Math.pow;
import static java.lang.Math.round;

public final class DecimalOperators
{
    public static final DecimalAddOperator ADD_OPERATOR = new DecimalAddOperator();

    private DecimalOperators()
    {
    }

    public static class DecimalAddOperator
            extends ParametricOperator
    {
        private static final MethodHandle ADD_METHOD_HANDLE = Reflection.methodHandle(DecimalAddOperator.class, "add", long.class, long.class, long.class, long.class);

        protected DecimalAddOperator()
        {
            super(ADD, ImmutableList.of(
                    comparableWithVariadicBound("A", DECIMAL),
                    comparableWithVariadicBound("B", DECIMAL)), DECIMAL, ImmutableList.of("A", "B"));
        }

        @Override
        public FunctionInfo specialize(Map<String, Type> types, int arity, TypeManager typeManager, FunctionRegistry functionRegistry)
        {
            checkArgument(arity == 2, "Expected arity to be 2");
            DecimalType aType = (DecimalType) types.get("A");
            DecimalType bType = (DecimalType) types.get("B");

            int aScale = aType.getScale();
            int bScale = bType.getScale();
            int aPrecision = aType.getPrecision();
            int bPrecision = bType.getPrecision();
            int resultPrecision = min(
                    DecimalType.MAX_PRECISION,
                    1 + max(aScale, bScale) + max(aPrecision - aScale, bPrecision - bScale));
            int resultScale = max(aScale, bScale);

            long aRescale = round(pow(10, resultScale - aScale));
            long bRescale = round(pow(10, resultScale - bScale));

            DecimalType resultType = DecimalType.createDecimalType(resultPrecision, resultScale);

            MethodHandle methodHandle = MethodHandles.insertArguments(ADD_METHOD_HANDLE, 2, aRescale, bRescale);

            return operatorInfo(ADD, resultType.getTypeSignature(), ImmutableList.of(aType.getTypeSignature(), bType.getTypeSignature()), methodHandle, false, ImmutableList.of(false, false));
        }

        public static long add(long a, long b, long aRescale, long bRescale)
        {
            return a * aRescale + b * bRescale;
        }
    }
}
