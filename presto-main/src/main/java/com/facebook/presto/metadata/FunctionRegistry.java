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

import com.facebook.presto.operator.Description;
import com.facebook.presto.operator.aggregation.AggregationFunction;
import com.facebook.presto.operator.aggregation.ApproximateCountDistinctAggregation;
import com.facebook.presto.operator.aggregation.DoubleApproximatePercentileAggregation;
import com.facebook.presto.operator.aggregation.DoubleApproximatePercentileWeightedAggregation;
import com.facebook.presto.operator.aggregation.DoubleStdDevAggregation;
import com.facebook.presto.operator.aggregation.DoubleVarianceAggregation;
import com.facebook.presto.operator.aggregation.LongApproximatePercentileAggregation;
import com.facebook.presto.operator.aggregation.LongApproximatePercentileWeightedAggregation;
import com.facebook.presto.operator.aggregation.LongStdDevAggregation;
import com.facebook.presto.operator.aggregation.LongVarianceAggregation;
import com.facebook.presto.operator.scalar.ColorFunctions;
import com.facebook.presto.operator.scalar.JsonFunctions;
import com.facebook.presto.operator.scalar.MathFunctions;
import com.facebook.presto.operator.scalar.RegexpFunctions;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.operator.scalar.StringFunctions;
import com.facebook.presto.operator.scalar.UnixTimeFunctions;
import com.facebook.presto.operator.scalar.UrlFunctions;
import com.facebook.presto.operator.window.CumulativeDistributionFunction;
import com.facebook.presto.operator.window.DenseRankFunction;
import com.facebook.presto.operator.window.PercentRankFunction;
import com.facebook.presto.operator.window.RankFunction;
import com.facebook.presto.operator.window.RowNumberFunction;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.gen.FunctionBinder;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;

import java.lang.invoke.MethodHandle;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.metadata.FunctionInfo.isAggregationPredicate;
import static com.facebook.presto.operator.aggregation.ApproximateAverageAggregation.DOUBLE_APPROXIMATE_AVERAGE_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximateAverageAggregation.LONG_APPROXIMATE_AVERAGE_AGGREGATION;
import static com.facebook.presto.operator.aggregation.BooleanMaxAggregation.BOOLEAN_MAX;
import static com.facebook.presto.operator.aggregation.BooleanMinAggregation.BOOLEAN_MIN;
import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.operator.aggregation.CountColumnAggregation.COUNT_BOOLEAN_COLUMN;
import static com.facebook.presto.operator.aggregation.CountColumnAggregation.COUNT_DOUBLE_COLUMN;
import static com.facebook.presto.operator.aggregation.CountColumnAggregation.COUNT_LONG_COLUMN;
import static com.facebook.presto.operator.aggregation.CountColumnAggregation.COUNT_STRING_COLUMN;
import static com.facebook.presto.operator.aggregation.CountIfAggregation.COUNT_IF;
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
import static com.facebook.presto.sql.analyzer.Type.BIGINT;
import static com.facebook.presto.sql.analyzer.Type.BOOLEAN;
import static com.facebook.presto.sql.analyzer.Type.DOUBLE;
import static com.facebook.presto.sql.analyzer.Type.VARCHAR;
import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

public class FunctionRegistry
{
    private final Multimap<QualifiedName, FunctionInfo> functionsByName;
    private final Map<FunctionHandle, FunctionInfo> functionsByHandle;

    public FunctionRegistry()
    {
        List<FunctionInfo> functions = new FunctionListBuilder()
                .window("row_number", BIGINT, ImmutableList.<Type>of(), supplier(RowNumberFunction.class))
                .window("rank", BIGINT, ImmutableList.<Type>of(), supplier(RankFunction.class))
                .window("dense_rank", BIGINT, ImmutableList.<Type>of(), supplier(DenseRankFunction.class))
                .window("percent_rank", DOUBLE, ImmutableList.<Type>of(), supplier(PercentRankFunction.class))
                .window("cume_dist", DOUBLE, ImmutableList.<Type>of(), supplier(CumulativeDistributionFunction.class))
                .aggregate("count", BIGINT, ImmutableList.<Type>of(), BIGINT, COUNT)
                .aggregate("count", BIGINT, ImmutableList.<Type>of(BOOLEAN), BIGINT, COUNT_BOOLEAN_COLUMN)
                .aggregate("count", BIGINT, ImmutableList.<Type>of(BIGINT), BIGINT, COUNT_LONG_COLUMN)
                .aggregate("count", BIGINT, ImmutableList.<Type>of(DOUBLE), BIGINT, COUNT_DOUBLE_COLUMN)
                .aggregate("count", BIGINT, ImmutableList.<Type>of(VARCHAR), BIGINT, COUNT_STRING_COLUMN)
                .aggregate("count_if", BIGINT, ImmutableList.<Type>of(BOOLEAN), BIGINT, COUNT_IF)
                .aggregate("sum", BIGINT, ImmutableList.of(BIGINT), BIGINT, LONG_SUM)
                .aggregate("sum", DOUBLE, ImmutableList.of(DOUBLE), DOUBLE, DOUBLE_SUM)
                .aggregate("avg", DOUBLE, ImmutableList.of(DOUBLE), VARCHAR, DOUBLE_AVERAGE)
                .aggregate("avg", DOUBLE, ImmutableList.of(BIGINT), VARCHAR, LONG_AVERAGE)
                .aggregate("max", BOOLEAN, ImmutableList.of(BOOLEAN), BOOLEAN, BOOLEAN_MAX)
                .aggregate("max", BIGINT, ImmutableList.of(BIGINT), BIGINT, LONG_MAX)
                .aggregate("max", DOUBLE, ImmutableList.of(DOUBLE), DOUBLE, DOUBLE_MAX)
                .aggregate("max", VARCHAR, ImmutableList.of(VARCHAR), VARCHAR, VAR_BINARY_MAX)
                .aggregate("min", BOOLEAN, ImmutableList.of(BOOLEAN), BOOLEAN, BOOLEAN_MIN)
                .aggregate("min", BIGINT, ImmutableList.of(BIGINT), BIGINT, LONG_MIN)
                .aggregate("min", DOUBLE, ImmutableList.of(DOUBLE), DOUBLE, DOUBLE_MIN)
                .aggregate("min", VARCHAR, ImmutableList.of(VARCHAR), VARCHAR, VAR_BINARY_MIN)
                .aggregate("var_pop", DOUBLE, ImmutableList.of(DOUBLE), VARCHAR, DoubleVarianceAggregation.VARIANCE_POP_INSTANCE)
                .aggregate("var_pop", DOUBLE, ImmutableList.of(BIGINT), VARCHAR, LongVarianceAggregation.VARIANCE_POP_INSTANCE)
                .aggregate("var_samp", DOUBLE, ImmutableList.of(DOUBLE), VARCHAR, DoubleVarianceAggregation.VARIANCE_INSTANCE)
                .aggregate("var_samp", DOUBLE, ImmutableList.of(BIGINT), VARCHAR, LongVarianceAggregation.VARIANCE_INSTANCE)
                .aggregate("variance", DOUBLE, ImmutableList.of(DOUBLE), VARCHAR, DoubleVarianceAggregation.VARIANCE_INSTANCE)
                .aggregate("variance", DOUBLE, ImmutableList.of(BIGINT), VARCHAR, LongVarianceAggregation.VARIANCE_INSTANCE)
                .aggregate("stddev_pop", DOUBLE, ImmutableList.of(DOUBLE), VARCHAR, DoubleStdDevAggregation.STDDEV_POP_INSTANCE)
                .aggregate("stddev_pop", DOUBLE, ImmutableList.of(BIGINT), VARCHAR, LongStdDevAggregation.STDDEV_POP_INSTANCE)
                .aggregate("stddev_samp", DOUBLE, ImmutableList.of(DOUBLE), VARCHAR, DoubleStdDevAggregation.STDDEV_INSTANCE)
                .aggregate("stddev_samp", DOUBLE, ImmutableList.of(BIGINT), VARCHAR, LongStdDevAggregation.STDDEV_INSTANCE)
                .aggregate("stddev", DOUBLE, ImmutableList.of(DOUBLE), VARCHAR, DoubleStdDevAggregation.STDDEV_INSTANCE)
                .aggregate("stddev", DOUBLE, ImmutableList.of(BIGINT), VARCHAR, LongStdDevAggregation.STDDEV_INSTANCE)
                .aggregate("approx_distinct", BIGINT, ImmutableList.of(BOOLEAN), VARCHAR, ApproximateCountDistinctAggregation.LONG_INSTANCE)
                .aggregate("approx_distinct", BIGINT, ImmutableList.of(BIGINT), VARCHAR, ApproximateCountDistinctAggregation.LONG_INSTANCE)
                .aggregate("approx_distinct", BIGINT, ImmutableList.of(DOUBLE), VARCHAR, ApproximateCountDistinctAggregation.DOUBLE_INSTANCE)
                .aggregate("approx_distinct", BIGINT, ImmutableList.of(VARCHAR), VARCHAR, ApproximateCountDistinctAggregation.VARBINARY_INSTANCE)
                .aggregate("approx_percentile", BIGINT, ImmutableList.of(BIGINT, DOUBLE), VARCHAR, LongApproximatePercentileAggregation.INSTANCE)
                .aggregate("approx_percentile", BIGINT, ImmutableList.of(BIGINT, BIGINT, DOUBLE), VARCHAR, LongApproximatePercentileWeightedAggregation.INSTANCE)
                .aggregate("approx_percentile", DOUBLE, ImmutableList.of(DOUBLE, DOUBLE), VARCHAR, DoubleApproximatePercentileAggregation.INSTANCE)
                .aggregate("approx_percentile", DOUBLE, ImmutableList.of(DOUBLE, BIGINT, DOUBLE), VARCHAR, DoubleApproximatePercentileWeightedAggregation.INSTANCE)
                .aggregate("approx_avg", VARCHAR, ImmutableList.of(BIGINT), VARCHAR, LONG_APPROXIMATE_AVERAGE_AGGREGATION)
                .aggregate("approx_avg", VARCHAR, ImmutableList.of(DOUBLE), VARCHAR, DOUBLE_APPROXIMATE_AVERAGE_AGGREGATION)
                .scalar(StringFunctions.class)
                .scalar(RegexpFunctions.class)
                .scalar(UrlFunctions.class)
                .scalar(MathFunctions.class)
                .scalar(UnixTimeFunctions.class)
                .scalar(JsonFunctions.class)
                .scalar(ColorFunctions.class)
                .build();

        functionsByName = Multimaps.index(functions, FunctionInfo.nameGetter());
        functionsByHandle = Maps.uniqueIndex(functions, FunctionInfo.handleGetter());

        // Make sure all functions with the same name are aggregations or none of them are
        for (Map.Entry<QualifiedName, Collection<FunctionInfo>> entry : functionsByName.asMap().entrySet()) {
            Collection<FunctionInfo> infos = entry.getValue();
            Preconditions.checkState(Iterables.all(infos, isAggregationPredicate()) || !Iterables.any(infos, isAggregationPredicate()),
                    "'%s' is both an aggregation and a scalar function", entry.getKey());
        }
    }

    public List<FunctionInfo> list()
    {
        return ImmutableList.copyOf(functionsByName.values());
    }

    public boolean isAggregationFunction(QualifiedName name)
    {
        return Iterables.any(functionsByName.get(name), isAggregationPredicate());
    }

    public FunctionInfo get(QualifiedName name, List<Type> parameterTypes)
    {
        // search for exact match
        for (FunctionInfo functionInfo : functionsByName.get(name)) {
            if (functionInfo.getArgumentTypes().equals(parameterTypes)) {
                return functionInfo;
            }
        }

        // search for coerced match
        for (FunctionInfo functionInfo : functionsByName.get(name)) {
            if (canCoerce(parameterTypes, functionInfo)) {
                return functionInfo;
            }
        }

        List<String> expectedParameters = new ArrayList<>();
        for (FunctionInfo functionInfo : functionsByName.get(name)) {
            expectedParameters.add(format("%s(%s)", name, Joiner.on(", ").join(functionInfo.getArgumentTypes())));
        }
        String parameters = Joiner.on(", ").join(parameterTypes);
        String message = format("Function %s not registered", name);
        if (!expectedParameters.isEmpty()) {
            String expected = Joiner.on(", ").join(expectedParameters);
            message = format("Unexpected parameters (%s) for function %s. Expected: %s", parameters, name, expected);
        }
        throw new IllegalArgumentException(message);
    }

    private static boolean canCoerce(List<Type> parameterTypes, FunctionInfo functionInfo)
    {
        List<Type> functionArguments = functionInfo.getArgumentTypes();
        if (parameterTypes.size() != functionArguments.size()) {
            return false;
        }
        for (int i = 0; i < functionArguments.size(); i++) {
            Type functionArgument = functionArguments.get(i);
            Type parameterType = parameterTypes.get(i);
            if (functionArgument != parameterType && !(functionArgument == DOUBLE && parameterType == BIGINT)) {
                return false;
            }
        }
        return true;
    }

    public FunctionInfo get(FunctionHandle handle)
    {
        return functionsByHandle.get(handle);
    }

    private static List<Type> types(MethodHandle handle)
    {
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (Class<?> parameter : getParameterTypes(handle.type().parameterArray())) {
            types.add(type(parameter));
        }
        return types.build();
    }

    private static List<Class<?>> getParameterTypes(Class<?>... types)
    {
        ImmutableList<Class<?>> parameterTypes = ImmutableList.copyOf(types);
        if (!parameterTypes.isEmpty() && parameterTypes.get(0) == Session.class) {
            parameterTypes = parameterTypes.subList(1, parameterTypes.size());
        }
        return parameterTypes;
    }

    private static Type type(Class<?> clazz)
    {
        clazz = Primitives.unwrap(clazz);
        if (clazz == long.class) {
            return BIGINT;
        }
        if (clazz == double.class) {
            return DOUBLE;
        }
        if (clazz == Slice.class) {
            return VARCHAR;
        }
        if (clazz == boolean.class) {
            return BOOLEAN;
        }
        throw new IllegalArgumentException("Unhandled type: " + clazz.getName());
    }

    private static class FunctionListBuilder
    {
        private final List<FunctionInfo> functions = new ArrayList<>();

        public FunctionListBuilder window(String name, Type returnType, List<Type> argumentTypes, Supplier<WindowFunction> function)
        {
            name = name.toLowerCase();

            String description = getDescription(function.getClass());
            int id = functions.size() + 1;
            functions.add(new FunctionInfo(id, QualifiedName.of(name), description, returnType, argumentTypes, function));
            return this;
        }

        public FunctionListBuilder aggregate(String name, Type returnType, List<Type> argumentTypes, Type intermediateType, AggregationFunction function)
        {
            name = name.toLowerCase();

            String description = getDescription(function.getClass());
            int id = functions.size() + 1;
            functions.add(new FunctionInfo(id, QualifiedName.of(name), description, returnType, argumentTypes, intermediateType, function));
            return this;
        }

        public FunctionListBuilder scalar(String name, MethodHandle function, boolean deterministic, FunctionBinder functionBinder, String description)
        {
            name = name.toLowerCase();

            int id = functions.size() + 1;
            Type returnType = type(function.type().returnType());
            List<Type> argumentTypes = types(function);
            functions.add(new FunctionInfo(id, QualifiedName.of(name), description, returnType, argumentTypes, function, deterministic, functionBinder));
            return this;
        }

        public FunctionListBuilder scalar(Class<?> clazz)
        {
            try {
                boolean foundOne = false;
                for (Method method : clazz.getMethods()) {
                    ScalarFunction scalarFunction = method.getAnnotation(ScalarFunction.class);
                    if (scalarFunction == null) {
                        continue;
                    }
                    checkValidMethod(method);
                    MethodHandle methodHandle = lookup().unreflect(method);
                    String name = scalarFunction.value();
                    if (name.isEmpty()) {
                        name = camelToSnake(method.getName());
                    }
                    String description = getDescription(method);
                    FunctionBinder functionBinder = createFunctionBinder(method, scalarFunction);
                    scalar(name, methodHandle, scalarFunction.deterministic(), functionBinder, description);
                    for (String alias : scalarFunction.alias()) {
                        scalar(alias, methodHandle, scalarFunction.deterministic(), functionBinder, description);
                    }
                    foundOne = true;
                }
                checkArgument(foundOne, "Expected class %s to contain at least one method annotated with @%s", clazz.getName(), ScalarFunction.class.getSimpleName());
            }
            catch (IllegalAccessException e) {
                throw Throwables.propagate(e);
            }
            return this;
        }

        private FunctionBinder createFunctionBinder(Method method, ScalarFunction scalarFunction)
        {
            Class<? extends FunctionBinder> functionBinderClass = scalarFunction.functionBinder();
            try {
                // look for <init>(MethodHandle,boolean)
                Constructor<? extends FunctionBinder> constructor = functionBinderClass.getConstructor(MethodHandle.class, boolean.class);
                return constructor.newInstance(lookup().unreflect(method), method.isAnnotationPresent(Nullable.class));
            }
            catch (ReflectiveOperationException | RuntimeException ignored) {
            }

            try {
                // try with default constructor
                return functionBinderClass.newInstance();
            }
            catch (Exception e) {
            }

            throw new IllegalArgumentException("Unable to create function binder " + functionBinderClass.getName() + " for function " + method);
        }

        private static String getDescription(AnnotatedElement annotatedElement)
        {
            Description description = annotatedElement.getAnnotation(Description.class);
            return (description == null) ? null : description.value();
        }

        private static String camelToSnake(String name)
        {
            return LOWER_CAMEL.to(LOWER_UNDERSCORE, name);
        }

        private static final Set<Class<?>> SUPPORTED_TYPES = ImmutableSet.<Class<?>>of(long.class, double.class, Slice.class, boolean.class);

        private static void checkValidMethod(Method method)
        {
            String message = "@ScalarFunction method %s is not valid: ";

            checkArgument(Modifier.isStatic(method.getModifiers()), message + "must be static", method);

            checkArgument(SUPPORTED_TYPES.contains(Primitives.unwrap(method.getReturnType())), message + "return type not supported", method);
            if (method.getAnnotation(Nullable.class) != null) {
                checkArgument(!method.getReturnType().isPrimitive(), message + "annotated with @Nullable but has primitive return type", method);
            }
            else {
                checkArgument(!Primitives.isWrapperType(method.getReturnType()), "not annotated with @Nullable but has boxed primitive return type", method);
            }

            for (Class<?> type : getParameterTypes(method.getParameterTypes())) {
                checkArgument(SUPPORTED_TYPES.contains(type), message + "parameter type [%s] not supported", method, type.getName());
            }
        }

        public ImmutableList<FunctionInfo> build()
        {
            Collections.sort(functions);
            return ImmutableList.copyOf(functions);
        }
    }

    private static Supplier<WindowFunction> supplier(final Class<? extends WindowFunction> clazz)
    {
        return new Supplier<WindowFunction>()
        {
            @Override
            public WindowFunction get()
            {
                try {
                    return clazz.getConstructor().newInstance();
                }
                catch (ReflectiveOperationException e) {
                    throw Throwables.propagate(e);
                }
            }
        };
    }
}
