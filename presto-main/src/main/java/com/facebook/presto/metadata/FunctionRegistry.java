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
import com.facebook.presto.operator.scalar.ColorFunctions;
import com.facebook.presto.operator.scalar.DateTimeFunctions;
import com.facebook.presto.operator.scalar.HyperLogLogFunctions;
import com.facebook.presto.operator.scalar.JsonFunctions;
import com.facebook.presto.operator.scalar.MathFunctions;
import com.facebook.presto.operator.scalar.RegexpFunctions;
import com.facebook.presto.operator.scalar.ScalarFunction;
import com.facebook.presto.operator.scalar.ScalarOperator;
import com.facebook.presto.operator.scalar.StringFunctions;
import com.facebook.presto.operator.scalar.UrlFunctions;
import com.facebook.presto.operator.scalar.VarbinaryFunctions;
import com.facebook.presto.operator.window.CumulativeDistributionFunction;
import com.facebook.presto.operator.window.DenseRankFunction;
import com.facebook.presto.operator.window.PercentRankFunction;
import com.facebook.presto.operator.window.RankFunction;
import com.facebook.presto.operator.window.RowNumberFunction;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.sql.gen.DefaultFunctionBinder;
import com.facebook.presto.sql.gen.FunctionBinder;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.BigintOperators;
import com.facebook.presto.type.BooleanOperators;
import com.facebook.presto.type.DateOperators;
import com.facebook.presto.type.DateTimeOperators;
import com.facebook.presto.type.DoubleOperators;
import com.facebook.presto.type.IntervalDayTimeOperators;
import com.facebook.presto.type.IntervalYearMonthOperators;
import com.facebook.presto.type.SqlType;
import com.facebook.presto.type.TimeOperators;
import com.facebook.presto.type.TimeWithTimeZoneOperators;
import com.facebook.presto.type.TimestampOperators;
import com.facebook.presto.type.TimestampWithTimeZoneOperators;
import com.facebook.presto.type.VarbinaryOperators;
import com.facebook.presto.type.VarcharOperators;
import com.facebook.presto.util.IterableTransformer;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.base.Supplier;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.primitives.Primitives;
import io.airlift.slice.Slice;

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.lang.annotation.Annotation;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.metadata.FunctionInfo.isAggregationPredicate;
import static com.facebook.presto.metadata.FunctionInfo.isHiddenPredicate;
import static com.facebook.presto.operator.aggregation.ApproximateAverageAggregations.DOUBLE_APPROXIMATE_AVERAGE_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximateAverageAggregations.LONG_APPROXIMATE_AVERAGE_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximateCountAggregation.APPROXIMATE_COUNT_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximateCountColumnAggregations.BOOLEAN_APPROXIMATE_COUNT_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximateCountColumnAggregations.DOUBLE_APPROXIMATE_COUNT_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximateCountColumnAggregations.LONG_APPROXIMATE_COUNT_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximateCountColumnAggregations.VARBINARY_APPROXIMATE_COUNT_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximateCountDistinctAggregations.DOUBLE_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS;
import static com.facebook.presto.operator.aggregation.ApproximateCountDistinctAggregations.LONG_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS;
import static com.facebook.presto.operator.aggregation.ApproximateCountDistinctAggregations.VARBINARY_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS;
import static com.facebook.presto.operator.aggregation.ApproximateDoubleSumAggregation.DOUBLE_APPROXIMATE_SUM_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximateLongSumAggregation.LONG_APPROXIMATE_SUM_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximatePercentileAggregations.DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximatePercentileAggregations.LONG_APPROXIMATE_PERCENTILE_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximatePercentileWeightedAggregations.DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ApproximatePercentileWeightedAggregations.LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION;
import static com.facebook.presto.operator.aggregation.AverageAggregations.DOUBLE_AVERAGE;
import static com.facebook.presto.operator.aggregation.AverageAggregations.LONG_AVERAGE;
import static com.facebook.presto.operator.aggregation.BooleanMaxAggregation.BOOLEAN_MAX;
import static com.facebook.presto.operator.aggregation.BooleanMinAggregation.BOOLEAN_MIN;
import static com.facebook.presto.operator.aggregation.CountAggregation.COUNT;
import static com.facebook.presto.operator.aggregation.CountColumnAggregations.COUNT_BOOLEAN_COLUMN;
import static com.facebook.presto.operator.aggregation.CountColumnAggregations.COUNT_DOUBLE_COLUMN;
import static com.facebook.presto.operator.aggregation.CountColumnAggregations.COUNT_LONG_COLUMN;
import static com.facebook.presto.operator.aggregation.CountColumnAggregations.COUNT_STRING_COLUMN;
import static com.facebook.presto.operator.aggregation.CountIfAggregation.COUNT_IF;
import static com.facebook.presto.operator.aggregation.DoubleMaxAggregation.DOUBLE_MAX;
import static com.facebook.presto.operator.aggregation.DoubleMinAggregation.DOUBLE_MIN;
import static com.facebook.presto.operator.aggregation.DoubleSumAggregation.DOUBLE_SUM;
import static com.facebook.presto.operator.aggregation.HyperLogLogAggregations.BIGINT_APPROXIMATE_SET_AGGREGATION;
import static com.facebook.presto.operator.aggregation.HyperLogLogAggregations.DOUBLE_APPROXIMATE_SET_AGGREGATION;
import static com.facebook.presto.operator.aggregation.HyperLogLogAggregations.MERGE_HYPER_LOG_LOG_AGGREGATION;
import static com.facebook.presto.operator.aggregation.HyperLogLogAggregations.VARCHAR_APPROXIMATE_SET_AGGREGATION;
import static com.facebook.presto.operator.aggregation.LongMaxAggregation.LONG_MAX;
import static com.facebook.presto.operator.aggregation.LongMinAggregation.LONG_MIN;
import static com.facebook.presto.operator.aggregation.LongSumAggregation.LONG_SUM;
import static com.facebook.presto.operator.aggregation.VarBinaryMaxAggregation.VAR_BINARY_MAX;
import static com.facebook.presto.operator.aggregation.VarBinaryMinAggregation.VAR_BINARY_MIN;
import static com.facebook.presto.operator.aggregation.VarianceAggregations.DOUBLE_STDDEV_INSTANCE;
import static com.facebook.presto.operator.aggregation.VarianceAggregations.DOUBLE_STDDEV_POP_INSTANCE;
import static com.facebook.presto.operator.aggregation.VarianceAggregations.DOUBLE_VARIANCE_INSTANCE;
import static com.facebook.presto.operator.aggregation.VarianceAggregations.DOUBLE_VARIANCE_POP_INSTANCE;
import static com.facebook.presto.operator.aggregation.VarianceAggregations.LONG_STDDEV_INSTANCE;
import static com.facebook.presto.operator.aggregation.VarianceAggregations.LONG_STDDEV_POP_INSTANCE;
import static com.facebook.presto.operator.aggregation.VarianceAggregations.LONG_VARIANCE_INSTANCE;
import static com.facebook.presto.operator.aggregation.VarianceAggregations.LONG_VARIANCE_POP_INSTANCE;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.google.common.base.CaseFormat.LOWER_CAMEL;
import static com.google.common.base.CaseFormat.LOWER_UNDERSCORE;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;
import static java.lang.String.format;
import static java.lang.invoke.MethodHandles.lookup;

@ThreadSafe
public class FunctionRegistry
{
    private static final String MAGIC_LITERAL_FUNCTION_PREFIX = "$literal$";

    private final TypeManager typeManager;
    private volatile FunctionMap functions = new FunctionMap();

    public FunctionRegistry(TypeManager typeManager, boolean experimentalSyntaxEnabled)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");

        FunctionListBuilder builder = new FunctionListBuilder()
                .window("row_number", BIGINT, ImmutableList.<Type>of(), supplier(RowNumberFunction.class))
                .window("rank", BIGINT, ImmutableList.<Type>of(), supplier(RankFunction.class))
                .window("dense_rank", BIGINT, ImmutableList.<Type>of(), supplier(DenseRankFunction.class))
                .window("percent_rank", DOUBLE, ImmutableList.<Type>of(), supplier(PercentRankFunction.class))
                .window("cume_dist", DOUBLE, ImmutableList.<Type>of(), supplier(CumulativeDistributionFunction.class))
                .aggregate("count", BIGINT, ImmutableList.<Type>of(), BIGINT, COUNT)
                .aggregate("count", BIGINT, ImmutableList.of(BOOLEAN), BIGINT, COUNT_BOOLEAN_COLUMN)
                .aggregate("count", BIGINT, ImmutableList.of(BIGINT), BIGINT, COUNT_LONG_COLUMN)
                .aggregate("count", BIGINT, ImmutableList.of(DOUBLE), BIGINT, COUNT_DOUBLE_COLUMN)
                .aggregate("count", BIGINT, ImmutableList.of(VARCHAR), BIGINT, COUNT_STRING_COLUMN)
                .aggregate("count_if", BIGINT, ImmutableList.of(BOOLEAN), BIGINT, COUNT_IF)
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
                .aggregate("var_pop", DOUBLE, ImmutableList.of(DOUBLE), VARCHAR, DOUBLE_VARIANCE_POP_INSTANCE)
                .aggregate("var_pop", DOUBLE, ImmutableList.of(BIGINT), VARCHAR, LONG_VARIANCE_POP_INSTANCE)
                .aggregate("var_samp", DOUBLE, ImmutableList.of(DOUBLE), VARCHAR, DOUBLE_VARIANCE_INSTANCE)
                .aggregate("var_samp", DOUBLE, ImmutableList.of(BIGINT), VARCHAR, LONG_VARIANCE_INSTANCE)
                .aggregate("variance", DOUBLE, ImmutableList.of(DOUBLE), VARCHAR, DOUBLE_VARIANCE_INSTANCE)
                .aggregate("variance", DOUBLE, ImmutableList.of(BIGINT), VARCHAR, LONG_VARIANCE_INSTANCE)
                .aggregate("stddev_pop", DOUBLE, ImmutableList.of(DOUBLE), VARCHAR, DOUBLE_STDDEV_POP_INSTANCE)
                .aggregate("stddev_pop", DOUBLE, ImmutableList.of(BIGINT), VARCHAR, LONG_STDDEV_POP_INSTANCE)
                .aggregate("stddev_samp", DOUBLE, ImmutableList.of(DOUBLE), VARCHAR, DOUBLE_STDDEV_INSTANCE)
                .aggregate("stddev_samp", DOUBLE, ImmutableList.of(BIGINT), VARCHAR, LONG_STDDEV_INSTANCE)
                .aggregate("stddev", DOUBLE, ImmutableList.of(DOUBLE), VARCHAR, DOUBLE_STDDEV_INSTANCE)
                .aggregate("stddev", DOUBLE, ImmutableList.of(BIGINT), VARCHAR, LONG_STDDEV_INSTANCE)
                .aggregate("approx_distinct", BIGINT, ImmutableList.of(BOOLEAN), VARCHAR, LONG_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS)
                .aggregate("approx_distinct", BIGINT, ImmutableList.of(BIGINT), VARCHAR, LONG_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS)
                .aggregate("approx_distinct", BIGINT, ImmutableList.of(DOUBLE), VARCHAR, DOUBLE_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS)
                .aggregate("approx_distinct", BIGINT, ImmutableList.of(VARCHAR), VARCHAR, VARBINARY_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS)
                .aggregate("approx_set", HYPER_LOG_LOG, ImmutableList.of(BIGINT), HYPER_LOG_LOG, BIGINT_APPROXIMATE_SET_AGGREGATION)
                .aggregate("approx_set", HYPER_LOG_LOG, ImmutableList.of(VARCHAR), HYPER_LOG_LOG, VARCHAR_APPROXIMATE_SET_AGGREGATION)
                .aggregate("approx_set", HYPER_LOG_LOG, ImmutableList.of(DOUBLE), HYPER_LOG_LOG, DOUBLE_APPROXIMATE_SET_AGGREGATION)
                .aggregate("merge", HYPER_LOG_LOG, ImmutableList.of(HYPER_LOG_LOG), HYPER_LOG_LOG, MERGE_HYPER_LOG_LOG_AGGREGATION)
                .aggregate("approx_percentile", BIGINT, ImmutableList.of(BIGINT, DOUBLE), VARCHAR, LONG_APPROXIMATE_PERCENTILE_AGGREGATION)
                .aggregate("approx_percentile", BIGINT, ImmutableList.of(BIGINT, BIGINT, DOUBLE), VARCHAR, LONG_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION)
                .aggregate("approx_percentile", DOUBLE, ImmutableList.of(DOUBLE, DOUBLE), VARCHAR, DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION)
                .aggregate("approx_percentile", DOUBLE, ImmutableList.of(DOUBLE, BIGINT, DOUBLE), VARCHAR, DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION)
                .aggregate("approx_avg", VARCHAR, ImmutableList.of(BIGINT), VARCHAR, LONG_APPROXIMATE_AVERAGE_AGGREGATION)
                .aggregate("approx_avg", VARCHAR, ImmutableList.of(DOUBLE), VARCHAR, DOUBLE_APPROXIMATE_AVERAGE_AGGREGATION)
                .scalar(StringFunctions.class)
                .scalar(VarbinaryFunctions.class)
                .scalar(RegexpFunctions.class)
                .scalar(UrlFunctions.class)
                .scalar(MathFunctions.class)
                .scalar(DateTimeFunctions.class)
                .scalar(JsonFunctions.class)
                .scalar(ColorFunctions.class)
                .scalar(HyperLogLogFunctions.class)
                .scalar(BooleanOperators.class)
                .scalar(BigintOperators.class)
                .scalar(DoubleOperators.class)
                .scalar(VarcharOperators.class)
                .scalar(VarbinaryOperators.class)
                .scalar(DateOperators.class)
                .scalar(TimeOperators.class)
                .scalar(TimestampOperators.class)
                .scalar(IntervalDayTimeOperators.class)
                .scalar(IntervalYearMonthOperators.class)
                .scalar(TimeWithTimeZoneOperators.class)
                .scalar(TimestampWithTimeZoneOperators.class)
                .scalar(DateTimeOperators.class);

        if (experimentalSyntaxEnabled) {
            builder.approximateAggregate("avg", VARCHAR, ImmutableList.of(BIGINT), VARCHAR, LONG_APPROXIMATE_AVERAGE_AGGREGATION)
                    .approximateAggregate("avg", VARCHAR, ImmutableList.of(DOUBLE), VARCHAR, DOUBLE_APPROXIMATE_AVERAGE_AGGREGATION)
                    .approximateAggregate("sum", VARCHAR, ImmutableList.of(BIGINT), VARCHAR, LONG_APPROXIMATE_SUM_AGGREGATION)
                    .approximateAggregate("sum", VARCHAR, ImmutableList.of(DOUBLE), VARCHAR, DOUBLE_APPROXIMATE_SUM_AGGREGATION)
                    .approximateAggregate("count", VARCHAR, ImmutableList.<Type>of(), VARCHAR, APPROXIMATE_COUNT_AGGREGATION)
                    .approximateAggregate("count", VARCHAR, ImmutableList.of(BOOLEAN), VARCHAR, BOOLEAN_APPROXIMATE_COUNT_AGGREGATION)
                    .approximateAggregate("count", VARCHAR, ImmutableList.of(BIGINT), VARCHAR, LONG_APPROXIMATE_COUNT_AGGREGATION)
                    .approximateAggregate("count", VARCHAR, ImmutableList.of(DOUBLE), VARCHAR, DOUBLE_APPROXIMATE_COUNT_AGGREGATION)
                    .approximateAggregate("count", VARCHAR, ImmutableList.of(VARCHAR), VARCHAR, VARBINARY_APPROXIMATE_COUNT_AGGREGATION);
        }

        addFunctions(builder.getFunctions(), builder.getOperators());
    }

    public final synchronized void addFunctions(List<FunctionInfo> functions, Multimap<OperatorType, FunctionInfo> operators)
    {
        for (FunctionInfo function : functions) {
            checkArgument(this.functions.get(function.getSignature()) == null,
                    "Function already registered: %s", function.getSignature());
        }

        this.functions = new FunctionMap(this.functions, functions, operators);
    }

    public List<FunctionInfo> list()
    {
        return FluentIterable.from(functions.list())
                .filter(not(isHiddenPredicate()))
                .toList();
    }

    public boolean isAggregationFunction(QualifiedName name)
    {
        return Iterables.any(functions.get(name), isAggregationPredicate());
    }

    public FunctionInfo resolveFunction(QualifiedName name, List<? extends Type> parameterTypes, final boolean approximate)
    {
        List<FunctionInfo> candidates = IterableTransformer.on(functions.get(name)).select(new Predicate<FunctionInfo>() {
            @Override
            public boolean apply(FunctionInfo input)
            {
                return input.isScalar() || input.isApproximate() == approximate;
            }
        }).list();

        // search for exact match
        for (FunctionInfo functionInfo : candidates) {
            if (functionInfo.getArgumentTypes().equals(parameterTypes)) {
                return functionInfo;
            }
        }

        // search for coerced match
        for (FunctionInfo functionInfo : candidates) {
            if (canCoerce(parameterTypes, functionInfo.getArgumentTypes())) {
                return functionInfo;
            }
        }

        List<String> expectedParameters = new ArrayList<>();
        for (FunctionInfo functionInfo : candidates) {
            expectedParameters.add(format("%s(%s)", name, Joiner.on(", ").join(functionInfo.getArgumentTypes())));
        }
        String parameters = Joiner.on(", ").join(parameterTypes);
        String message = format("Function %s not registered", name);
        if (!expectedParameters.isEmpty()) {
            String expected = Joiner.on(", ").join(expectedParameters);
            message = format("Unexpected parameters (%s) for function %s. Expected: %s", parameters, name, expected);
        }

        if (name.getSuffix().startsWith(MAGIC_LITERAL_FUNCTION_PREFIX)) {
            // extract type from function name
            String typeName = name.getSuffix().substring(MAGIC_LITERAL_FUNCTION_PREFIX.length());

            // lookup the type
            Type type = typeManager.getType(typeName);
            checkArgument(type != null, "Type %s not registered", typeName);

            // verify we have one parameter of the proper type
            checkArgument(parameterTypes.size() == 1, "Expected one argument to literal function, but got %s", parameterTypes);
            Type parameterType = parameterTypes.get(0);
            checkArgument(parameterType.getJavaType() == type.getJavaType(),
                    "Expected type %s to use Java type %s, but Java type is %s",
                    type,
                    parameterType.getJavaType(),
                    type.getJavaType());

            MethodHandle identity = MethodHandles.identity(parameterTypes.get(0).getJavaType());
            return new FunctionInfo(
                    new Signature(MAGIC_LITERAL_FUNCTION_PREFIX, type, ImmutableList.copyOf(parameterTypes), false),
                    null,
                    true,
                    identity,
                    true,
                    new DefaultFunctionBinder(identity, false));
        }

        throw new PrestoException(StandardErrorCode.FUNCTION_NOT_FOUND.toErrorCode(), message);
    }

    public FunctionInfo getExactFunction(Signature signature)
    {
        return functions.get(signature);
    }

    public FunctionInfo resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        Iterable<FunctionInfo> candidates = functions.getOperators(operatorType);

        // search for exact match
        for (FunctionInfo operatorInfo : candidates) {
            if (operatorInfo.getArgumentTypes().equals(argumentTypes)) {
                return operatorInfo;
            }
        }

        // search for coerced match
        for (FunctionInfo operatorInfo : candidates) {
            if (canCoerce(argumentTypes, operatorInfo.getArgumentTypes())) {
                return operatorInfo;
            }
        }

        throw new OperatorNotFoundException(operatorType, argumentTypes);
    }

    public FunctionInfo getExactOperator(OperatorType operatorType, List<? extends Type> argumentTypes, Type returnType)
            throws OperatorNotFoundException
    {
        Iterable<FunctionInfo> candidates = functions.getOperators(operatorType);

        // search for exact match
        for (FunctionInfo operatorInfo : candidates) {
            if (operatorInfo.getReturnType().equals(returnType) && operatorInfo.getArgumentTypes().equals(argumentTypes)) {
                return operatorInfo;
            }
        }

        // if identity cast, return a custom operator info
        if ((operatorType == OperatorType.CAST) && (argumentTypes.size() == 1) && argumentTypes.get(0).equals(returnType)) {
            MethodHandle identity = MethodHandles.identity(returnType.getJavaType());
            return operatorInfo(OperatorType.CAST, returnType, argumentTypes, identity, new DefaultFunctionBinder(identity, false));
        }

        throw new OperatorNotFoundException(operatorType, argumentTypes, returnType);
    }

    public static boolean canCoerce(List<? extends Type> actualTypes, List<Type> expectedTypes)
    {
        if (actualTypes.size() != expectedTypes.size()) {
            return false;
        }
        for (int i = 0; i < expectedTypes.size(); i++) {
            Type expectedType = expectedTypes.get(i);
            Type actualType = actualTypes.get(i);
            if (!canCoerce(actualType, expectedType)) {
                return false;
            }
        }
        return true;
    }

    public static boolean canCoerce(Type actualType, Type expectedType)
    {
        // are types the same
        if (expectedType.equals(actualType)) {
            return true;
        }
        // null can be cast to anything
        if (actualType.equals(UNKNOWN)) {
            return true;
        }
        // widen bigint to double
        if (actualType.equals(BIGINT) && expectedType.equals(DOUBLE)) {
            return true;
        }
        // widen date to timestamp
        if (actualType.equals(DATE) && expectedType.equals(TIMESTAMP)) {
            return true;
        }
        // widen date to timestamp with time zone
        if (actualType.equals(DATE) && expectedType.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return true;
        }
        // widen time to time with time zone
        if (actualType.equals(TIME) && expectedType.equals(TIME_WITH_TIME_ZONE)) {
            return true;
        }
        // widen timestamp to timestamp with time zone
        if (actualType.equals(TIMESTAMP) && expectedType.equals(TIMESTAMP_WITH_TIME_ZONE)) {
            return true;
        }
        return false;
    }

    public static Optional<Type> getCommonSuperType(Type firstType, Type secondType)
    {
        if (firstType.equals(UNKNOWN)) {
            return Optional.of(secondType);
        }

        if (secondType.equals(UNKNOWN)) {
            return Optional.of(firstType);
        }

        if (firstType.equals(secondType)) {
            return Optional.of(firstType);
        }

        if ((firstType.equals(BIGINT) || firstType.equals(DOUBLE)) && (secondType.equals(BIGINT) || secondType.equals(DOUBLE))) {
            return Optional.<Type>of(DOUBLE);
        }

        if ((firstType.equals(TIME) || firstType.equals(TIME_WITH_TIME_ZONE)) && (secondType.equals(TIME) || secondType.equals(TIME_WITH_TIME_ZONE))) {
            return Optional.<Type>of(TIME_WITH_TIME_ZONE);
        }

        if ((firstType.equals(TIMESTAMP) || firstType.equals(TIMESTAMP_WITH_TIME_ZONE)) && (secondType.equals(TIMESTAMP) || secondType.equals(TIMESTAMP_WITH_TIME_ZONE))) {
            return Optional.<Type>of(TIMESTAMP_WITH_TIME_ZONE);
        }

        return Optional.absent();
    }

    private static List<Type> parameterTypes(Method method)
    {
        Annotation[][] parameterAnnotations = method.getParameterAnnotations();

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (int i = 0; i < method.getParameterTypes().length; i++) {
            Class<?> clazz = method.getParameterTypes()[i];
            // skip session parameters
            if (clazz == ConnectorSession.class) {
                continue;
            }

            // find the explicit type annotation if present
            SqlType explicitType = null;
            for (Annotation annotation : parameterAnnotations[i]) {
                if (annotation instanceof SqlType) {
                    explicitType = (SqlType) annotation;
                    break;
                }
            }
            checkArgument(explicitType != null, "Method %s argument %s does not have a @SqlType annotation", method, i);
            types.add(type(explicitType));
        }
        return types.build();
    }

    private static List<Class<?>> getParameterTypes(Class<?>... types)
    {
        ImmutableList<Class<?>> parameterTypes = ImmutableList.copyOf(types);
        if (!parameterTypes.isEmpty() && parameterTypes.get(0) == ConnectorSession.class) {
            parameterTypes = parameterTypes.subList(1, parameterTypes.size());
        }
        return parameterTypes;
    }

    private static Type type(SqlType explicitType)
    {
        try {
            return (Type) explicitType.value().getMethod("getInstance").invoke(null);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public static Type type(Class<?> clazz)
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
        throw new IllegalArgumentException("Unhandled Java type: " + clazz.getName());
    }

    public static Signature getMagicLiteralFunctionSignature(Type type)
    {
        return new Signature(MAGIC_LITERAL_FUNCTION_PREFIX + type.getName(),
                type,
                ImmutableList.of(type(type.getJavaType())),
                false);
    }

    public static class FunctionListBuilder
    {
        private final List<FunctionInfo> functions = new ArrayList<>();
        private final Multimap<OperatorType, FunctionInfo> operators = ArrayListMultimap.create();

        public FunctionListBuilder window(String name, Type returnType, List<? extends Type> argumentTypes, Supplier<WindowFunction> function)
        {
            name = name.toLowerCase();

            String description = getDescription(function.getClass());
            functions.add(new FunctionInfo(new Signature(name, returnType, ImmutableList.copyOf(argumentTypes), false), description, function));
            return this;
        }

        public FunctionListBuilder approximateAggregate(String name, Type returnType, List<? extends Type> argumentTypes, Type intermediateType, AggregationFunction function)
        {
            return aggregate(name, returnType, argumentTypes, true, intermediateType, function);
        }

        public FunctionListBuilder aggregate(String name, Type returnType, List<? extends Type> argumentTypes, Type intermediateType, AggregationFunction function)
        {
            return aggregate(name, returnType, argumentTypes, false, intermediateType, function);
        }

        private FunctionListBuilder aggregate(String name, Type returnType, List<? extends Type> argumentTypes, boolean approximate, Type intermediateType,
                AggregationFunction function)
        {
            name = name.toLowerCase();

            String description = getDescription(function.getClass());
            functions.add(new FunctionInfo(new Signature(name, returnType, ImmutableList.copyOf(argumentTypes), approximate), description, intermediateType, function));
            return this;
        }

        public FunctionListBuilder scalar(Signature signature, MethodHandle function, boolean deterministic, FunctionBinder functionBinder, String description, boolean hidden)
        {
            functions.add(new FunctionInfo(signature, description, hidden, function, deterministic, functionBinder));
            return this;
        }

        private FunctionListBuilder operator(OperatorType operatorType, Type returnType, List<Type> parameterTypes, MethodHandle function, FunctionBinder functionBinder)
        {
            operators.put(operatorType, operatorInfo(operatorType, returnType, parameterTypes, function, functionBinder));
            return this;
        }

        public FunctionListBuilder scalar(Class<?> clazz)
        {
            try {
                boolean foundOne = false;
                for (Method method : clazz.getMethods()) {
                    foundOne = processScalarFunction(method) || foundOne;
                    foundOne = processScalarOperator(method) || foundOne;
                }
                checkArgument(foundOne, "Expected class %s to contain at least one method annotated with @%s", clazz.getName(), ScalarFunction.class.getSimpleName());
            }
            catch (IllegalAccessException e) {
                throw Throwables.propagate(e);
            }
            return this;
        }

        private boolean processScalarFunction(Method method)
                throws IllegalAccessException
        {
            ScalarFunction scalarFunction = method.getAnnotation(ScalarFunction.class);
            if (scalarFunction == null) {
                return false;
            }
            checkValidMethod(method);
            MethodHandle methodHandle = lookup().unreflect(method);
            String name = scalarFunction.value();
            if (name.isEmpty()) {
                name = camelToSnake(method.getName());
            }
            SqlType returnTypeAnnotation = method.getAnnotation(SqlType.class);
            checkArgument(returnTypeAnnotation != null, "Method %s return type does not have a @SqlType annotation", method);
            Type returnType = type(returnTypeAnnotation);
            Signature signature = new Signature(name.toLowerCase(), returnType, parameterTypes(method), false);

            verifyMethodSignature(method, signature.getReturnType(), signature.getArgumentTypes());

            FunctionBinder functionBinder = createFunctionBinder(method, scalarFunction.functionBinder());

            scalar(signature, methodHandle, scalarFunction.deterministic(), functionBinder, getDescription(method), scalarFunction.hidden());
            for (String alias : scalarFunction.alias()) {
                scalar(signature.withAlias(alias.toLowerCase()), methodHandle, scalarFunction.deterministic(), functionBinder, getDescription(method), scalarFunction.hidden());
            }
            return true;
        }

        private boolean processScalarOperator(Method method)
                throws IllegalAccessException
        {
            ScalarOperator scalarOperator = method.getAnnotation(ScalarOperator.class);
            if (scalarOperator == null) {
                return false;
            }
            checkValidMethod(method);
            MethodHandle methodHandle = lookup().unreflect(method);
            OperatorType operatorType = scalarOperator.value();

            List<Type> parameterTypes = parameterTypes(method);

            Type returnType;
            if (operatorType == OperatorType.HASH_CODE) {
                // todo hack for hashCode... should be int
                returnType = BIGINT;
            }
            else {
                SqlType explicitType = method.getAnnotation(SqlType.class);
                checkArgument(explicitType != null, "Method %s return type does not have a @SqlType annotation", method);
                returnType = type(explicitType);

                verifyMethodSignature(method, returnType, parameterTypes);
            }

            FunctionBinder functionBinder = createFunctionBinder(method, scalarOperator.functionBinder());

            operator(operatorType, returnType, parameterTypes, methodHandle, functionBinder);
            return true;
        }

        private FunctionBinder createFunctionBinder(Method method, Class<? extends FunctionBinder> functionBinderClass)
        {
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
        private static final Set<Class<?>> SUPPORTED_RETURN_TYPES = ImmutableSet.<Class<?>>of(long.class, double.class, Slice.class, boolean.class, int.class);

        private static void checkValidMethod(Method method)
        {
            String message = "@ScalarFunction method %s is not valid: ";

            checkArgument(Modifier.isStatic(method.getModifiers()), message + "must be static", method);

            checkArgument(SUPPORTED_RETURN_TYPES.contains(Primitives.unwrap(method.getReturnType())), message + "return type not supported", method);
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

        public List<FunctionInfo> getFunctions()
        {
            return ImmutableList.copyOf(functions);
        }

        public Multimap<OperatorType, FunctionInfo> getOperators()
        {
            return ImmutableMultimap.copyOf(operators);
        }
    }

    private static FunctionInfo operatorInfo(OperatorType operatorType, Type returnType, List<? extends Type> argumentTypes, MethodHandle method, FunctionBinder functionBinder)
    {
        operatorType.validateSignature(returnType, ImmutableList.copyOf(argumentTypes));

        Signature signature = new Signature(operatorType.name(), returnType, argumentTypes, false, true);
        return new FunctionInfo(signature, operatorType.getOperator(), true, method, true, functionBinder);
    }

    private static void verifyMethodSignature(Method method, Type returnType, List<Type> argumentTypes)
    {
        checkArgument(Primitives.unwrap(method.getReturnType()) == returnType.getJavaType(),
                "Expected method %s return type to be %s (%s)",
                method,
                returnType.getJavaType().getName(),
                returnType);

        // skip Session argument
        Class<?>[] parameterTypes = method.getParameterTypes();
        if (parameterTypes.length > 0 && parameterTypes[0] == ConnectorSession.class) {
            parameterTypes = Arrays.copyOfRange(parameterTypes, 1, parameterTypes.length);
        }

        for (int i = 0; i < parameterTypes.length; i++) {
            Class<?> actualType = parameterTypes[i];
            Type expectedType = argumentTypes.get(i);
            checkArgument(Primitives.unwrap(actualType) == expectedType.getJavaType(),
                    "Expected method %s parameter %s type to be %s (%s)",
                    method,
                    i,
                    expectedType.getJavaType().getName(),
                    expectedType);
        }
    }

    public static Supplier<WindowFunction> supplier(final Class<? extends WindowFunction> clazz)
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

    private static class FunctionMap
    {
        private final Multimap<QualifiedName, FunctionInfo> functionsByName;
        private final Map<Signature, FunctionInfo> functionsBySignature;
        private final Multimap<OperatorType, FunctionInfo> byOperator;

        public FunctionMap()
        {
            functionsByName = ImmutableListMultimap.of();
            functionsBySignature = ImmutableMap.of();
            byOperator = ImmutableListMultimap.of();
        }

        public FunctionMap(FunctionMap map, Iterable<FunctionInfo> functions, Multimap<OperatorType, FunctionInfo> operators)
        {
            functionsByName = ImmutableListMultimap.<QualifiedName, FunctionInfo>builder()
                    .putAll(map.functionsByName)
                    .putAll(Multimaps.index(functions, FunctionInfo.nameGetter()))
                    .build();

            functionsBySignature = ImmutableMap.<Signature, FunctionInfo>builder()
                    .putAll(map.functionsBySignature)
                    .putAll(Maps.uniqueIndex(functions, FunctionInfo.handleGetter()))
                    .build();

            byOperator = ImmutableListMultimap.<OperatorType, FunctionInfo>builder()
                    .putAll(map.byOperator)
                    .putAll(operators)
                    .build();

            // Make sure all functions with the same name are aggregations or none of them are
            for (Map.Entry<QualifiedName, Collection<FunctionInfo>> entry : functionsByName.asMap().entrySet()) {
                Collection<FunctionInfo> infos = entry.getValue();
                checkState(Iterables.all(infos, isAggregationPredicate()) || !Iterables.any(infos, isAggregationPredicate()),
                        "'%s' is both an aggregation and a scalar function", entry.getKey());
            }
        }

        public List<FunctionInfo> list()
        {
            return ImmutableList.copyOf(functionsByName.values());
        }

        public Collection<FunctionInfo> get(QualifiedName name)
        {
            return functionsByName.get(name);
        }

        public FunctionInfo get(Signature signature)
        {
            return functionsBySignature.get(signature);
        }

        public Collection<FunctionInfo> getOperators(OperatorType operatorType)
        {
            return byOperator.get(operatorType);
        }
    }
}
