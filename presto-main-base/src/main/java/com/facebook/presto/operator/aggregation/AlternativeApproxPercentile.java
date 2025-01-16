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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.BooleanType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.metadata.BuiltInFunction;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.metadata.SqlAggregationFunction;
import com.google.common.collect.ImmutableList;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

public final class AlternativeApproxPercentile
{
    private AlternativeApproxPercentile() {}

    public static BuiltInFunction[] getFunctions()
    {
        List<BuiltInFunction> functions = new ArrayList<>();
        boolean[] ft = {false, true};
        for (Type type : new Type[] {BigintType.BIGINT, RealType.REAL, DoubleType.DOUBLE}) {
            for (boolean percentileArray : ft) {
                for (boolean weighted : ft) {
                    for (boolean accuracy : ft) {
                        functions.add(new Function(type, percentileArray, weighted, accuracy));
                    }
                }
            }
        }
        return functions.toArray(new BuiltInFunction[0]);
    }

    private static Type createReturnType(Type valueType, boolean percentileArray)
    {
        return percentileArray ? new ArrayType(valueType) : valueType;
    }

    private static List<Type> createArgumentTypes(
            Type valueType, boolean percentileArray, boolean weighted, boolean accuracy)
    {
        List<Type> arguments = new ArrayList<>();
        arguments.add(valueType);
        if (weighted) {
            arguments.add(BigintType.BIGINT);
        }
        if (percentileArray) {
            arguments.add(new ArrayType(DoubleType.DOUBLE));
        }
        else {
            arguments.add(DoubleType.DOUBLE);
        }
        if (accuracy) {
            arguments.add(DoubleType.DOUBLE);
        }
        return arguments;
    }

    private static class Function
            extends SqlAggregationFunction
    {
        private static final String NAME = "approx_percentile";
        private final Type valueType;
        private final List<Type> inputTypes;
        private final Type outputType;

        Function(Type valueType, boolean percentileArray, boolean weighted, boolean accuracy)
        {
            super(
                    NAME,
                    Collections.emptyList(),
                    Collections.emptyList(),
                    createReturnType(valueType, percentileArray).getTypeSignature(),
                    createArgumentTypes(valueType, percentileArray, weighted, accuracy).stream().map(Type::getTypeSignature).collect(ImmutableList.toImmutableList()));
            this.valueType = valueType;
            inputTypes = createArgumentTypes(valueType, percentileArray, weighted, accuracy);
            outputType = createReturnType(valueType, percentileArray);
        }

        public String getDescription()
        {
            return "";
        }

        @Override
        public BuiltInAggregationFunctionImplementation specialize(
                BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
        {
            Type intermediateType = RowType.from(
                    ImmutableList.of(
                            new RowType.Field(Optional.of("percentiles"), new ArrayType(DoubleType.DOUBLE)),
                            new RowType.Field(Optional.of("percentileArray"), BooleanType.BOOLEAN),
                            new RowType.Field(Optional.of("accuracy"), DoubleType.DOUBLE),
                            new RowType.Field(Optional.of("k"), IntegerType.INTEGER),
                            new RowType.Field(Optional.of("n"), BigintType.BIGINT),
                            new RowType.Field(Optional.of("minValue"), valueType),
                            new RowType.Field(Optional.of("maxValue"), valueType),
                            new RowType.Field(Optional.of("items"), new ArrayType(valueType)),
                            new RowType.Field(Optional.of("levels"), new ArrayType(IntegerType.INTEGER))));
            return new BuiltInAggregationFunctionImplementation(
                    NAME, inputTypes, ImmutableList.of(intermediateType), outputType,
                    true, false, null, null, null);
        }
    }
}
