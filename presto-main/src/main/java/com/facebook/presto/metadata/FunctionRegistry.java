package com.facebook.presto.metadata;

import com.facebook.presto.operator.scalar.StringFunctions;
import com.facebook.presto.slice.Slice;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodType;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.operator.aggregation.DoubleAverageAggregation.DOUBLE_AVERAGE;
import static com.facebook.presto.operator.aggregation.DoubleMaxAggregation.DOUBLE_MAX;
import static com.facebook.presto.operator.aggregation.DoubleMinAggregation.DOUBLE_MIN;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;
import static com.facebook.presto.operator.aggregation.LongAverageAggregation.LONG_AVERAGE;
import static com.facebook.presto.operator.aggregation.LongMaxAggregation.LONG_MAX;
import static com.facebook.presto.operator.aggregation.LongMinAggregation.LONG_MIN;
import static com.facebook.presto.operator.aggregation.LongSumAggregation.LONG_SUM;
import static com.facebook.presto.operator.aggregation.VarBinaryMaxAggregation.VAR_BINARY_MAX;
import static com.facebook.presto.operator.aggregation.VarBinaryMinAggregation.VAR_BINARY_MIN;
import static com.facebook.presto.tuple.TupleInfo.Type.DOUBLE;
import static com.facebook.presto.tuple.TupleInfo.Type.FIXED_INT_64;
import static com.facebook.presto.tuple.TupleInfo.Type.VARIABLE_BINARY;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

public class FunctionRegistry
{
    private final Multimap<QualifiedName, FunctionInfo> functionsByName;
    private final Map<FunctionHandle, FunctionInfo> functionsByHandle;

    public FunctionRegistry()
    {
        List<FunctionInfo> functions = ImmutableList.of(
                new FunctionInfo(1, QualifiedName.of("count"), FIXED_INT_64, ImmutableList.<TupleInfo.Type>of(), FIXED_INT_64, COUNT),
                new FunctionInfo(2, QualifiedName.of("sum"), FIXED_INT_64, ImmutableList.of(FIXED_INT_64), FIXED_INT_64, LONG_SUM),
                new FunctionInfo(3, QualifiedName.of("sum"), DOUBLE, ImmutableList.of(DOUBLE), DOUBLE, DOUBLE_SUM),
                new FunctionInfo(4, QualifiedName.of("avg"), DOUBLE, ImmutableList.of(DOUBLE), VARIABLE_BINARY, DOUBLE_AVERAGE),
                new FunctionInfo(5, QualifiedName.of("avg"), DOUBLE, ImmutableList.of(FIXED_INT_64), VARIABLE_BINARY, LONG_AVERAGE),
                new FunctionInfo(6, QualifiedName.of("max"), FIXED_INT_64, ImmutableList.of(FIXED_INT_64), FIXED_INT_64, LONG_MAX),
                new FunctionInfo(7, QualifiedName.of("max"), DOUBLE, ImmutableList.of(DOUBLE), DOUBLE, DOUBLE_MAX),
                new FunctionInfo(8, QualifiedName.of("max"), VARIABLE_BINARY, ImmutableList.of(VARIABLE_BINARY), VARIABLE_BINARY, VAR_BINARY_MAX),
                new FunctionInfo(9, QualifiedName.of("min"), FIXED_INT_64, ImmutableList.of(FIXED_INT_64), FIXED_INT_64, LONG_MIN),
                new FunctionInfo(10, QualifiedName.of("min"), DOUBLE, ImmutableList.of(DOUBLE), DOUBLE, DOUBLE_MIN),
                new FunctionInfo(11, QualifiedName.of("min"), VARIABLE_BINARY, ImmutableList.of(VARIABLE_BINARY), VARIABLE_BINARY, VAR_BINARY_MIN),
                new FunctionInfo(12, QualifiedName.of("substr"), VARIABLE_BINARY, types(StringFunctions.SUBSTR), StringFunctions.SUBSTR)
        );

        functionsByName = Multimaps.index(functions, FunctionInfo.nameGetter());
        functionsByHandle = Maps.uniqueIndex(functions, FunctionInfo.handleGetter());
    }

    public FunctionInfo get(QualifiedName name, List<TupleInfo.Type> parameterTypes)
    {
        for (FunctionInfo functionInfo : functionsByName.get(name)) {
            if (functionInfo.getArgumentTypes().equals(parameterTypes)) {
                return functionInfo;
            }
        }

        throw new IllegalArgumentException(format("Function %s(%s) not registered", name, Joiner.on(", ").join(parameterTypes)));
    }

    public FunctionInfo get(FunctionHandle handle)
    {
        return functionsByHandle.get(handle);
    }

    public static MethodHandle lookupStatic(Class<?> clazz, String methodName, Class<?> returnType, Class<?>... parameterTypes)
    {
        try {
            return lookup().findStatic(clazz, methodName, MethodType.methodType(returnType, parameterTypes));
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static List<TupleInfo.Type> types(MethodHandle handle)
    {
        ImmutableList.Builder<TupleInfo.Type> types = ImmutableList.builder();
        for (Class<?> parameter : handle.type().parameterList()) {
            if (parameter == long.class) {
                types.add(FIXED_INT_64);
            }
            else if (parameter == double.class) {
                types.add(DOUBLE);
            }
            else if (parameter == Slice.class) {
                types.add(VARIABLE_BINARY);
            }
            else {
                throw new IllegalArgumentException("Unhandled parameter type: " + parameter.getName());
            }
        }
        return types.build();
    }
}
