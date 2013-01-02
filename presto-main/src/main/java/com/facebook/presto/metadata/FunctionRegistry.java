package com.facebook.presto.metadata;

import com.facebook.presto.operator.aggregation.AggregationFunction;
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
import java.util.ArrayList;
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
        List<FunctionInfo> functions = new FunctionListBuilder()
                .aggregate("count", FIXED_INT_64, ImmutableList.<TupleInfo.Type>of(), FIXED_INT_64, COUNT)
                .aggregate("sum", FIXED_INT_64, ImmutableList.of(FIXED_INT_64), FIXED_INT_64, LONG_SUM)
                .aggregate("sum", DOUBLE, ImmutableList.of(DOUBLE), DOUBLE, DOUBLE_SUM)
                .aggregate("avg", DOUBLE, ImmutableList.of(DOUBLE), VARIABLE_BINARY, DOUBLE_AVERAGE)
                .aggregate("avg", DOUBLE, ImmutableList.of(FIXED_INT_64), VARIABLE_BINARY, LONG_AVERAGE)
                .aggregate("max", FIXED_INT_64, ImmutableList.of(FIXED_INT_64), FIXED_INT_64, LONG_MAX)
                .aggregate("max", DOUBLE, ImmutableList.of(DOUBLE), DOUBLE, DOUBLE_MAX)
                .aggregate("max", VARIABLE_BINARY, ImmutableList.of(VARIABLE_BINARY), VARIABLE_BINARY, VAR_BINARY_MAX)
                .aggregate("min", FIXED_INT_64, ImmutableList.of(FIXED_INT_64), FIXED_INT_64, LONG_MIN)
                .aggregate("min", DOUBLE, ImmutableList.of(DOUBLE), DOUBLE, DOUBLE_MIN)
                .aggregate("min", VARIABLE_BINARY, ImmutableList.of(VARIABLE_BINARY), VARIABLE_BINARY, VAR_BINARY_MIN)
                .scalar("concat", StringFunctions.CONCAT)
                .scalar("length", StringFunctions.LENGTH)
                .scalar("reverse", StringFunctions.REVERSE)
                .scalar("substr", StringFunctions.SUBSTR)
                .scalar("ltrim", StringFunctions.LTRIM)
                .scalar("rtrim", StringFunctions.RTRIM)
                .scalar("trim", StringFunctions.TRIM)
                .scalar("lower", StringFunctions.LOWER)
                .scalar("upper", StringFunctions.UPPER)
                .build();

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

        String parameters = Joiner.on(", ").useForNull("NULL").join(parameterTypes);
        throw new IllegalArgumentException(format("Function %s(%s) not registered", name, parameters));
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
            types.add(type(parameter));
        }
        return types.build();
    }

    private static TupleInfo.Type type(Class<?> clazz)
    {
        if (clazz == long.class) {
            return FIXED_INT_64;
        }
        if (clazz == double.class) {
            return DOUBLE;
        }
        if (clazz == Slice.class) {
            return VARIABLE_BINARY;
        }
        throw new IllegalArgumentException("Unhandled type: " + clazz.getName());
    }

    private static class FunctionListBuilder
    {
        private final List<FunctionInfo> list = new ArrayList<>();

        public FunctionListBuilder aggregate(String name, TupleInfo.Type returnType, List<TupleInfo.Type> argumentTypes, TupleInfo.Type intermediateType, AggregationFunction function)
        {
            int id = list.size() + 1;
            list.add(new FunctionInfo(id, QualifiedName.of(name), returnType, argumentTypes, intermediateType, function));
            return this;
        }

        public FunctionListBuilder scalar(String name, MethodHandle function)
        {
            int id = list.size() + 1;
            TupleInfo.Type returnType = type(function.type().returnType());
            List<TupleInfo.Type> argumentTypes = types(function);
            list.add(new FunctionInfo(id, QualifiedName.of(name), returnType, argumentTypes, function));
            return this;
        }

        public ImmutableList<FunctionInfo> build()
        {
            return ImmutableList.copyOf(list);
        }
    }
}
