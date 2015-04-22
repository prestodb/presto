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

import com.facebook.presto.operator.aggregation.ApproximateAverageAggregations;
import com.facebook.presto.operator.aggregation.ApproximateCountAggregation;
import com.facebook.presto.operator.aggregation.ApproximateCountColumnAggregations;
import com.facebook.presto.operator.aggregation.ApproximateCountDistinctAggregations;
import com.facebook.presto.operator.aggregation.ApproximateDoublePercentileAggregations;
import com.facebook.presto.operator.aggregation.ApproximateLongPercentileAggregations;
import com.facebook.presto.operator.aggregation.ApproximateSetAggregation;
import com.facebook.presto.operator.aggregation.ApproximateSumAggregations;
import com.facebook.presto.operator.aggregation.AverageAggregations;
import com.facebook.presto.operator.aggregation.BooleanAndAggregation;
import com.facebook.presto.operator.aggregation.BooleanOrAggregation;
import com.facebook.presto.operator.aggregation.CountAggregation;
import com.facebook.presto.operator.aggregation.CountIfAggregation;
import com.facebook.presto.operator.aggregation.CorrelationAggregation;
import com.facebook.presto.operator.aggregation.CovarianceAggregation;
import com.facebook.presto.operator.aggregation.DoubleSumAggregation;
import com.facebook.presto.operator.aggregation.LongSumAggregation;
import com.facebook.presto.operator.aggregation.MergeHyperLogLogAggregation;
import com.facebook.presto.operator.aggregation.NumericHistogramAggregation;
import com.facebook.presto.operator.aggregation.RegressionAggregation;
import com.facebook.presto.operator.aggregation.VarianceAggregation;
import com.facebook.presto.operator.scalar.ArrayFunctions;
import com.facebook.presto.operator.scalar.ColorFunctions;
import com.facebook.presto.operator.scalar.CombineHashFunction;
import com.facebook.presto.operator.scalar.DateTimeFunctions;
import com.facebook.presto.operator.scalar.HyperLogLogFunctions;
import com.facebook.presto.operator.scalar.JsonFunctions;
import com.facebook.presto.operator.scalar.JsonOperators;
import com.facebook.presto.operator.scalar.MathFunctions;
import com.facebook.presto.operator.scalar.RegexpFunctions;
import com.facebook.presto.operator.scalar.StringFunctions;
import com.facebook.presto.operator.scalar.UrlFunctions;
import com.facebook.presto.operator.scalar.VarbinaryFunctions;
import com.facebook.presto.operator.window.CumulativeDistributionFunction;
import com.facebook.presto.operator.window.DenseRankFunction;
import com.facebook.presto.operator.window.FirstValueFunction.BigintFirstValueFunction;
import com.facebook.presto.operator.window.FirstValueFunction.BooleanFirstValueFunction;
import com.facebook.presto.operator.window.FirstValueFunction.DoubleFirstValueFunction;
import com.facebook.presto.operator.window.FirstValueFunction.VarcharFirstValueFunction;
import com.facebook.presto.operator.window.LagFunction.BigintLagFunction;
import com.facebook.presto.operator.window.LagFunction.BooleanLagFunction;
import com.facebook.presto.operator.window.LagFunction.DoubleLagFunction;
import com.facebook.presto.operator.window.LagFunction.VarcharLagFunction;
import com.facebook.presto.operator.window.LastValueFunction.BigintLastValueFunction;
import com.facebook.presto.operator.window.LastValueFunction.BooleanLastValueFunction;
import com.facebook.presto.operator.window.LastValueFunction.DoubleLastValueFunction;
import com.facebook.presto.operator.window.LastValueFunction.VarcharLastValueFunction;
import com.facebook.presto.operator.window.LeadFunction.BigintLeadFunction;
import com.facebook.presto.operator.window.LeadFunction.BooleanLeadFunction;
import com.facebook.presto.operator.window.LeadFunction.DoubleLeadFunction;
import com.facebook.presto.operator.window.LeadFunction.VarcharLeadFunction;
import com.facebook.presto.operator.window.NTileFunction;
import com.facebook.presto.operator.window.NthValueFunction.BigintNthValueFunction;
import com.facebook.presto.operator.window.NthValueFunction.BooleanNthValueFunction;
import com.facebook.presto.operator.window.NthValueFunction.DoubleNthValueFunction;
import com.facebook.presto.operator.window.NthValueFunction.VarcharNthValueFunction;
import com.facebook.presto.operator.window.PercentRankFunction;
import com.facebook.presto.operator.window.RankFunction;
import com.facebook.presto.operator.window.RowNumberFunction;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.ArrayType;
import com.facebook.presto.type.BigintOperators;
import com.facebook.presto.type.BooleanOperators;
import com.facebook.presto.type.DateOperators;
import com.facebook.presto.type.DateTimeOperators;
import com.facebook.presto.type.DoubleOperators;
import com.facebook.presto.type.HyperLogLogOperators;
import com.facebook.presto.type.IntervalDayTimeOperators;
import com.facebook.presto.type.IntervalYearMonthOperators;
import com.facebook.presto.type.LikeFunctions;
import com.facebook.presto.type.RowParametricType;
import com.facebook.presto.type.RowType;
import com.facebook.presto.type.TimeOperators;
import com.facebook.presto.type.TimeWithTimeZoneOperators;
import com.facebook.presto.type.TimestampOperators;
import com.facebook.presto.type.TimestampWithTimeZoneOperators;
import com.facebook.presto.type.VarbinaryOperators;
import com.facebook.presto.type.VarcharOperators;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.primitives.Primitives;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.Slice;

import javax.annotation.concurrent.ThreadSafe;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.operator.aggregation.ArbitraryAggregation.ARBITRARY_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ArrayAggregation.ARRAY_AGGREGATION;
import static com.facebook.presto.operator.aggregation.CountColumn.COUNT_COLUMN;
import static com.facebook.presto.operator.aggregation.MapAggregation.MAP_AGG;
import static com.facebook.presto.operator.aggregation.MaxAggregation.MAX_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MaxBy.MAX_BY;
import static com.facebook.presto.operator.aggregation.MinAggregation.MIN_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MinBy.MIN_BY;
import static com.facebook.presto.operator.scalar.ArrayCardinalityFunction.ARRAY_CARDINALITY;
import static com.facebook.presto.operator.scalar.ArrayConcatFunction.ARRAY_CONCAT_FUNCTION;
import static com.facebook.presto.operator.scalar.ArrayConstructor.ARRAY_CONSTRUCTOR;
import static com.facebook.presto.operator.scalar.ArrayContains.ARRAY_CONTAINS;
import static com.facebook.presto.operator.scalar.ArrayDistinctFunction.ARRAY_DISTINCT_FUNCTION;
import static com.facebook.presto.operator.scalar.ArrayEqualOperator.ARRAY_EQUAL;
import static com.facebook.presto.operator.scalar.ArrayGreaterThanOperator.ARRAY_GREATER_THAN;
import static com.facebook.presto.operator.scalar.ArrayGreaterThanOrEqualOperator.ARRAY_GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.operator.scalar.ArrayHashCodeOperator.ARRAY_HASH_CODE;
import static com.facebook.presto.operator.scalar.ArrayIntersectFunction.ARRAY_INTERSECT_FUNCTION;
import static com.facebook.presto.operator.scalar.ArrayJoin.ARRAY_JOIN;
import static com.facebook.presto.operator.scalar.ArrayJoin.ARRAY_JOIN_WITH_NULL_REPLACEMENT;
import static com.facebook.presto.operator.scalar.ArrayLessThanOperator.ARRAY_LESS_THAN;
import static com.facebook.presto.operator.scalar.ArrayLessThanOrEqualOperator.ARRAY_LESS_THAN_OR_EQUAL;
import static com.facebook.presto.operator.scalar.ArrayNotEqualOperator.ARRAY_NOT_EQUAL;
import static com.facebook.presto.operator.scalar.ArrayPositionFunction.ARRAY_POSITION;
import static com.facebook.presto.operator.scalar.ArraySortFunction.ARRAY_SORT_FUNCTION;
import static com.facebook.presto.operator.scalar.ArrayRemoveFunction.ARRAY_REMOVE_FUNCTION;
import static com.facebook.presto.operator.scalar.ArraySubscriptOperator.ARRAY_SUBSCRIPT;
import static com.facebook.presto.operator.scalar.ArrayToArrayCast.ARRAY_TO_ARRAY_CAST;
import static com.facebook.presto.operator.scalar.ArrayToElementConcatFunction.ARRAY_TO_ELEMENT_CONCAT_FUNCTION;
import static com.facebook.presto.operator.scalar.ArrayToJsonCast.ARRAY_TO_JSON;
import static com.facebook.presto.operator.scalar.ElementToArrayConcatFunction.ELEMENT_TO_ARRAY_CONCAT_FUNCTION;
import static com.facebook.presto.operator.scalar.Greatest.GREATEST;
import static com.facebook.presto.operator.scalar.IdentityCast.IDENTITY_CAST;
import static com.facebook.presto.operator.scalar.JsonToArrayCast.JSON_TO_ARRAY;
import static com.facebook.presto.operator.scalar.JsonToMapCast.JSON_TO_MAP;
import static com.facebook.presto.operator.scalar.Least.LEAST;
import static com.facebook.presto.operator.scalar.MapCardinalityFunction.MAP_CARDINALITY;
import static com.facebook.presto.operator.scalar.MapConstructor.MAP_CONSTRUCTOR;
import static com.facebook.presto.operator.scalar.MapEqualOperator.MAP_EQUAL;
import static com.facebook.presto.operator.scalar.MapHashCodeOperator.MAP_HASH_CODE;
import static com.facebook.presto.operator.scalar.MapKeys.MAP_KEYS;
import static com.facebook.presto.operator.scalar.MapNotEqualOperator.MAP_NOT_EQUAL;
import static com.facebook.presto.operator.scalar.MapSubscriptOperator.MAP_SUBSCRIPT;
import static com.facebook.presto.operator.scalar.MapToJsonCast.MAP_TO_JSON;
import static com.facebook.presto.operator.scalar.MapValues.MAP_VALUES;
import static com.facebook.presto.operator.scalar.RowEqualOperator.ROW_EQUAL;
import static com.facebook.presto.operator.scalar.RowHashCodeOperator.ROW_HASH_CODE;
import static com.facebook.presto.operator.scalar.RowNotEqualOperator.ROW_NOT_EQUAL;
import static com.facebook.presto.operator.scalar.RowToJsonCast.ROW_TO_JSON;
import static com.facebook.presto.operator.scalar.TryCastFunction.TRY_CAST;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.TimeType.TIME;
import static com.facebook.presto.spi.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.type.JsonPathType.JSON_PATH;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static com.facebook.presto.type.RegexpType.REGEXP;
import static com.facebook.presto.type.TypeUtils.resolveTypes;
import static com.facebook.presto.type.UnknownType.UNKNOWN;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;
import static java.lang.String.format;

@ThreadSafe
public class FunctionRegistry
{
    private static final String MAGIC_LITERAL_FUNCTION_PREFIX = "$literal$";
    private static final String OPERATOR_PREFIX = "$operator$";

    // hack: java classes for types that can be used with magic literals
    private static final Set<Class<?>> SUPPORTED_LITERAL_TYPES = ImmutableSet.<Class<?>>of(long.class, double.class, Slice.class, boolean.class);

    private final TypeManager typeManager;
    private final BlockEncodingSerde blockEncodingSerde;
    private final LoadingCache<SpecializedFunctionKey, FunctionInfo> specializedFunctionCache;
    private volatile FunctionMap functions = new FunctionMap();

    public FunctionRegistry(TypeManager typeManager, BlockEncodingSerde blockEncodingSerde, boolean experimentalSyntaxEnabled)
    {
        this.typeManager = checkNotNull(typeManager, "typeManager is null");
        this.blockEncodingSerde = checkNotNull(blockEncodingSerde, "blockEncodingSerde is null");

        specializedFunctionCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(new CacheLoader<SpecializedFunctionKey, FunctionInfo>()
                {
                    @Override
                    public FunctionInfo load(SpecializedFunctionKey key)
                            throws Exception
                    {
                        return key.getFunction().specialize(key.getBoundTypeParameters(), key.getArity(), typeManager, FunctionRegistry.this);
                    }
                });

        FunctionListBuilder builder = new FunctionListBuilder(typeManager)
                .window("row_number", BIGINT, ImmutableList.<Type>of(), RowNumberFunction.class)
                .window("rank", BIGINT, ImmutableList.<Type>of(), RankFunction.class)
                .window("dense_rank", BIGINT, ImmutableList.<Type>of(), DenseRankFunction.class)
                .window("percent_rank", DOUBLE, ImmutableList.<Type>of(), PercentRankFunction.class)
                .window("cume_dist", DOUBLE, ImmutableList.<Type>of(), CumulativeDistributionFunction.class)
                .window("ntile", BIGINT, ImmutableList.<Type>of(BIGINT), NTileFunction.class)
                .window("first_value", BIGINT, ImmutableList.<Type>of(BIGINT), BigintFirstValueFunction.class)
                .window("first_value", DOUBLE, ImmutableList.<Type>of(DOUBLE), DoubleFirstValueFunction.class)
                .window("first_value", BOOLEAN, ImmutableList.<Type>of(BOOLEAN), BooleanFirstValueFunction.class)
                .window("first_value", VARCHAR, ImmutableList.<Type>of(VARCHAR), VarcharFirstValueFunction.class)
                .window("last_value", BIGINT, ImmutableList.<Type>of(BIGINT), BigintLastValueFunction.class)
                .window("last_value", DOUBLE, ImmutableList.<Type>of(DOUBLE), DoubleLastValueFunction.class)
                .window("last_value", BOOLEAN, ImmutableList.<Type>of(BOOLEAN), BooleanLastValueFunction.class)
                .window("last_value", VARCHAR, ImmutableList.<Type>of(VARCHAR), VarcharLastValueFunction.class)
                .window("nth_value", BIGINT, ImmutableList.<Type>of(BIGINT, BIGINT), BigintNthValueFunction.class)
                .window("nth_value", DOUBLE, ImmutableList.<Type>of(DOUBLE, BIGINT), DoubleNthValueFunction.class)
                .window("nth_value", BOOLEAN, ImmutableList.<Type>of(BOOLEAN, BIGINT), BooleanNthValueFunction.class)
                .window("nth_value", VARCHAR, ImmutableList.<Type>of(VARCHAR, BIGINT), VarcharNthValueFunction.class)
                .window("lag", BIGINT, ImmutableList.<Type>of(BIGINT), BigintLagFunction.class)
                .window("lag", BIGINT, ImmutableList.<Type>of(BIGINT, BIGINT), BigintLagFunction.class)
                .window("lag", BIGINT, ImmutableList.<Type>of(BIGINT, BIGINT, BIGINT), BigintLagFunction.class)
                .window("lag", DOUBLE, ImmutableList.<Type>of(DOUBLE), DoubleLagFunction.class)
                .window("lag", DOUBLE, ImmutableList.<Type>of(DOUBLE, BIGINT), DoubleLagFunction.class)
                .window("lag", DOUBLE, ImmutableList.<Type>of(DOUBLE, BIGINT, DOUBLE), DoubleLagFunction.class)
                .window("lag", BOOLEAN, ImmutableList.<Type>of(BOOLEAN), BooleanLagFunction.class)
                .window("lag", BOOLEAN, ImmutableList.<Type>of(BOOLEAN, BIGINT), BooleanLagFunction.class)
                .window("lag", BOOLEAN, ImmutableList.<Type>of(BOOLEAN, BIGINT, BOOLEAN), BooleanLagFunction.class)
                .window("lag", VARCHAR, ImmutableList.<Type>of(VARCHAR), VarcharLagFunction.class)
                .window("lag", VARCHAR, ImmutableList.<Type>of(VARCHAR, BIGINT), VarcharLagFunction.class)
                .window("lag", VARCHAR, ImmutableList.<Type>of(VARCHAR, BIGINT, VARCHAR), VarcharLagFunction.class)
                .window("lead", BIGINT, ImmutableList.<Type>of(BIGINT), BigintLeadFunction.class)
                .window("lead", BIGINT, ImmutableList.<Type>of(BIGINT, BIGINT), BigintLeadFunction.class)
                .window("lead", BIGINT, ImmutableList.<Type>of(BIGINT, BIGINT, BIGINT), BigintLeadFunction.class)
                .window("lead", DOUBLE, ImmutableList.<Type>of(DOUBLE), DoubleLeadFunction.class)
                .window("lead", DOUBLE, ImmutableList.<Type>of(DOUBLE, BIGINT), DoubleLeadFunction.class)
                .window("lead", DOUBLE, ImmutableList.<Type>of(DOUBLE, BIGINT, DOUBLE), DoubleLeadFunction.class)
                .window("lead", BOOLEAN, ImmutableList.<Type>of(BOOLEAN), BooleanLeadFunction.class)
                .window("lead", BOOLEAN, ImmutableList.<Type>of(BOOLEAN, BIGINT), BooleanLeadFunction.class)
                .window("lead", BOOLEAN, ImmutableList.<Type>of(BOOLEAN, BIGINT, BOOLEAN), BooleanLeadFunction.class)
                .window("lead", VARCHAR, ImmutableList.<Type>of(VARCHAR), VarcharLeadFunction.class)
                .window("lead", VARCHAR, ImmutableList.<Type>of(VARCHAR, BIGINT), VarcharLeadFunction.class)
                .window("lead", VARCHAR, ImmutableList.<Type>of(VARCHAR, BIGINT, VARCHAR), VarcharLeadFunction.class)
                .aggregate(CountAggregation.class)
                .aggregate(VarianceAggregation.class)
                .aggregate(ApproximateLongPercentileAggregations.class)
                .aggregate(ApproximateDoublePercentileAggregations.class)
                .aggregate(CountIfAggregation.class)
                .aggregate(BooleanAndAggregation.class)
                .aggregate(BooleanOrAggregation.class)
                .aggregate(DoubleSumAggregation.class)
                .aggregate(LongSumAggregation.class)
                .aggregate(AverageAggregations.class)
                .aggregate(ApproximateCountDistinctAggregations.class)
                .aggregate(MergeHyperLogLogAggregation.class)
                .aggregate(ApproximateSetAggregation.class)
                .aggregate(NumericHistogramAggregation.class)
                .aggregate(CovarianceAggregation.class)
                .aggregate(RegressionAggregation.class)
                .aggregate(CorrelationAggregation.class)
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
                .scalar(DateTimeOperators.class)
                .scalar(HyperLogLogOperators.class)
                .scalar(LikeFunctions.class)
                .scalar(ArrayFunctions.class)
                .scalar(CombineHashFunction.class)
                .scalar(JsonOperators.class)
                .function(IDENTITY_CAST)
                .functions(ARRAY_CONTAINS, ARRAY_JOIN, ARRAY_JOIN_WITH_NULL_REPLACEMENT)
                .functions(ARRAY_TO_ARRAY_CAST, ARRAY_HASH_CODE, ARRAY_EQUAL, ARRAY_NOT_EQUAL, ARRAY_LESS_THAN, ARRAY_LESS_THAN_OR_EQUAL, ARRAY_GREATER_THAN, ARRAY_GREATER_THAN_OR_EQUAL)
                .functions(ARRAY_CONCAT_FUNCTION, ARRAY_TO_ELEMENT_CONCAT_FUNCTION, ELEMENT_TO_ARRAY_CONCAT_FUNCTION)
                .functions(MAP_EQUAL, MAP_NOT_EQUAL, MAP_HASH_CODE)
                .functions(ARRAY_CONSTRUCTOR, ARRAY_SUBSCRIPT, ARRAY_CARDINALITY, ARRAY_POSITION, ARRAY_SORT_FUNCTION, ARRAY_INTERSECT_FUNCTION, ARRAY_TO_JSON, JSON_TO_ARRAY, ARRAY_DISTINCT_FUNCTION, ARRAY_REMOVE_FUNCTION)
                .functions(MAP_CONSTRUCTOR, MAP_CARDINALITY, MAP_SUBSCRIPT, MAP_TO_JSON, JSON_TO_MAP, MAP_KEYS, MAP_VALUES, MAP_AGG)
                .function(ARBITRARY_AGGREGATION)
                .function(ARRAY_AGGREGATION)
                .function(LEAST)
                .function(GREATEST)
                .function(MAX_BY)
                .function(MIN_BY)
                .functions(MAX_AGGREGATION, MIN_AGGREGATION)
                .function(COUNT_COLUMN)
                .functions(ROW_HASH_CODE, ROW_TO_JSON, ROW_EQUAL, ROW_NOT_EQUAL)
                .function(TRY_CAST);

        if (experimentalSyntaxEnabled) {
            builder.aggregate(ApproximateAverageAggregations.class)
                    .aggregate(ApproximateSumAggregations.class)
                    .aggregate(ApproximateCountAggregation.class)
                    .aggregate(ApproximateCountColumnAggregations.class);
        }

        addFunctions(builder.getFunctions());
    }

    public final synchronized void addFunctions(List<? extends ParametricFunction> functions)
    {
        for (ParametricFunction function : functions) {
            for (ParametricFunction existingFunction : this.functions.list()) {
                checkArgument(!function.getSignature().equals(existingFunction.getSignature()), "Function already registered: %s", function.getSignature());
            }
        }
        this.functions = new FunctionMap(this.functions, functions);
    }

    public List<ParametricFunction> list()
    {
        return FluentIterable.from(functions.list())
                .filter(not(ParametricFunction::isHidden))
                .toList();
    }

    public boolean isAggregationFunction(QualifiedName name)
    {
        return Iterables.any(functions.get(name), ParametricFunction::isAggregate);
    }

    public FunctionInfo resolveFunction(QualifiedName name, List<TypeSignature> parameterTypes, boolean approximate)
    {
        List<ParametricFunction> candidates = functions.get(name).stream()
                .filter(function -> function.isScalar() || function.isApproximate() == approximate)
                .collect(toImmutableList());

        List<Type> resolvedTypes = resolveTypes(parameterTypes, typeManager);
        // search for exact match
        FunctionInfo match = null;
        for (ParametricFunction function : candidates) {
            Map<String, Type> boundTypeParameters = function.getSignature().bindTypeParameters(resolvedTypes, false, typeManager);
            if (boundTypeParameters != null) {
                checkArgument(match == null, "Ambiguous call to %s with parameters %s", name, parameterTypes);
                try {
                    match = specializedFunctionCache.getUnchecked(new SpecializedFunctionKey(function, boundTypeParameters, resolvedTypes.size()));
                }
                catch (UncheckedExecutionException e) {
                    throw Throwables.propagate(e.getCause());
                }
            }
        }

        if (match != null) {
            return match;
        }

        // search for coerced match
        for (ParametricFunction function : candidates) {
            Map<String, Type> boundTypeParameters = function.getSignature().bindTypeParameters(resolvedTypes, true, typeManager);
            if (boundTypeParameters != null) {
                // TODO: This should also check for ambiguities
                try {
                    return specializedFunctionCache.getUnchecked(new SpecializedFunctionKey(function, boundTypeParameters, resolvedTypes.size()));
                }
                catch (UncheckedExecutionException e) {
                    throw Throwables.propagate(e.getCause());
                }
            }
        }

        List<String> expectedParameters = new ArrayList<>();
        for (ParametricFunction function : candidates) {
            expectedParameters.add(format("%s(%s) %s",
                                    name,
                                    Joiner.on(", ").join(function.getSignature().getArgumentTypes()),
                                    Joiner.on(", ").join(function.getSignature().getTypeParameters())));
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
            Type type = typeManager.getType(parseTypeSignature(typeName));
            checkNotNull(type, "Type %s not registered", typeName);

            // verify we have one parameter of the proper type
            checkArgument(parameterTypes.size() == 1, "Expected one argument to literal function, but got %s", parameterTypes);
            Type parameterType = typeManager.getType(parameterTypes.get(0));
            checkNotNull(parameterType, "Type %s not found", parameterTypes.get(0));
            checkArgument(parameterType.getJavaType() == type.getJavaType(),
                    "Expected type %s to use Java type %s, but Java type is %s",
                    type,
                    parameterType.getJavaType(),
                    type.getJavaType());

            MethodHandle identity = MethodHandles.identity(parameterType.getJavaType());
            return new FunctionInfo(
                    getMagicLiteralFunctionSignature(type),
                    null,
                    true,
                    identity,
                    true,
                    false,
                    ImmutableList.of(false));
        }

        // TODO this should be made to work for any parametric type
        for (TypeSignature typeSignature : parameterTypes) {
            if (typeSignature.getBase().equals(StandardTypes.ROW)) {
                RowType rowType = RowParametricType.ROW.createType(resolveTypes(typeSignature.getParameters(), typeManager), typeSignature.getLiteralParameters());
                // search for exact match
                for (ParametricFunction function : RowParametricType.ROW.createFunctions(rowType)) {
                    if (!function.getSignature().getName().equals(name.toString())) {
                        continue;
                    }
                    Map<String, Type> boundTypeParameters = function.getSignature().bindTypeParameters(resolvedTypes, false, typeManager);
                    if (boundTypeParameters != null) {
                        checkArgument(match == null, "Ambiguous call to %s with parameters %s", name, parameterTypes);
                        try {
                            match = specializedFunctionCache.getUnchecked(new SpecializedFunctionKey(function, boundTypeParameters, resolvedTypes.size()));
                        }
                        catch (UncheckedExecutionException e) {
                            throw Throwables.propagate(e.getCause());
                        }
                    }
                }

                if (match != null) {
                    return match;
                }
            }
        }

        throw new PrestoException(FUNCTION_NOT_FOUND, message);
    }

    public FunctionInfo getExactFunction(Signature signature)
    {
        Iterable<ParametricFunction> candidates = functions.get(QualifiedName.of(signature.getName()));
        // search for exact match
        for (ParametricFunction operator : candidates) {
            Type returnType = typeManager.getType(signature.getReturnType());
            List<Type> argumentTypes = resolveTypes(signature.getArgumentTypes(), typeManager);
            Map<String, Type> boundTypeParameters = operator.getSignature().bindTypeParameters(returnType, argumentTypes, false, typeManager);
            if (boundTypeParameters != null) {
                try {
                    return specializedFunctionCache.getUnchecked(new SpecializedFunctionKey(operator, boundTypeParameters, signature.getArgumentTypes().size()));
                }
                catch (UncheckedExecutionException e) {
                    throw Throwables.propagate(e.getCause());
                }
            }
        }
        return null;
    }

    @VisibleForTesting
    public List<ParametricFunction> listOperators()
    {
        Set<String> operatorNames = Arrays.asList(OperatorType.values()).stream()
                .map(FunctionRegistry::mangleOperatorName)
                .collect(toImmutableSet());

        return functions.list().stream()
                .filter(function -> operatorNames.contains(function.getSignature().getName()))
                .collect(toImmutableList());
    }

    public FunctionInfo resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        try {
            return resolveFunction(QualifiedName.of(mangleOperatorName(operatorType)), Lists.transform(argumentTypes, Type::getTypeSignature), false);
        }
        catch (PrestoException e) {
            if (e.getErrorCode().getCode() == FUNCTION_NOT_FOUND.toErrorCode().getCode()) {
                throw new OperatorNotFoundException(operatorType, argumentTypes);
            }
            else {
                throw e;
            }
        }
    }

    public FunctionInfo getCoercion(Type fromType, Type toType)
    {
        FunctionInfo functionInfo = getExactFunction(Signature.internalOperator(OperatorType.CAST.name(), toType.getTypeSignature(), ImmutableList.of(fromType.getTypeSignature())));
        if (functionInfo == null) {
            throw new OperatorNotFoundException(OperatorType.CAST, ImmutableList.of(fromType), toType);
        }
        return functionInfo;
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

        if (actualType.equals(VARCHAR) && expectedType.equals(REGEXP)) {
            return true;
        }

        if (actualType.equals(VARCHAR) && expectedType.equals(LIKE_PATTERN)) {
            return true;
        }

        if (actualType.equals(VARCHAR) && expectedType.equals(JSON_PATH)) {
            return true;
        }

        if (actualType instanceof ArrayType && expectedType instanceof ArrayType) {
            Type actualElementType = ((ArrayType) actualType).getElementType();
            Type expectedElementType = ((ArrayType) expectedType).getElementType();
            return canCoerce(actualElementType, expectedElementType);
        }

        return false;
    }

    public static Optional<Type> getCommonSuperType(List<? extends Type> types)
    {
        checkArgument(!types.isEmpty(), "types is empty");
        Type superType = UNKNOWN;
        for (Type type : types) {
            Optional<Type> commonSuperType = getCommonSuperType(superType, type);
            if (!commonSuperType.isPresent()) {
                return Optional.empty();
            }
            superType = commonSuperType.get();
        }
        return Optional.of(superType);
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

        if (firstType instanceof ArrayType && secondType instanceof ArrayType) {
            Optional<Type> elementType = getCommonSuperType(((ArrayType) firstType).getElementType(), ((ArrayType) secondType).getElementType());
            if (elementType.isPresent()) {
                return Optional.of(new ArrayType(elementType.get()));
            }
        }

        // TODO add row and map type

        return Optional.empty();
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
        TypeSignature argumentType;
        if (type.getJavaType() == Slice.class && !type.equals(VARCHAR)) {
            argumentType = VARBINARY.getTypeSignature();
        }
        else {
            argumentType = type(type.getJavaType()).getTypeSignature();
        }

        return new Signature(MAGIC_LITERAL_FUNCTION_PREFIX + type.getTypeSignature(),
                type.getTypeSignature(),
                argumentType);
    }

    public static boolean isSupportedLiteralType(Type type)
    {
        return SUPPORTED_LITERAL_TYPES.contains(type.getJavaType());
    }

    public static FunctionInfo operatorInfo(OperatorType operatorType, TypeSignature returnType, List<TypeSignature> argumentTypes, MethodHandle method, boolean nullable, List<Boolean> nullableArguments)
    {
        operatorType.validateSignature(returnType, argumentTypes);

        Signature signature = Signature.internalOperator(operatorType.name(), returnType, argumentTypes);
        return new FunctionInfo(signature, operatorType.getOperator(), true, method, true, nullable, nullableArguments);
    }

    public static String mangleOperatorName(OperatorType operatorType)
    {
        return mangleOperatorName(operatorType.name());
    }

    public static String mangleOperatorName(String operatorName)
    {
        return OPERATOR_PREFIX + operatorName;
    }

    @VisibleForTesting
    public static OperatorType unmangleOperator(String mangledName)
    {
        checkArgument(mangledName.startsWith(OPERATOR_PREFIX), "%s is not a mangled operator name", mangledName);
        return OperatorType.valueOf(mangledName.substring(OPERATOR_PREFIX.length()));
    }

    private static class FunctionMap
    {
        private final Multimap<QualifiedName, ParametricFunction> functions;

        public FunctionMap()
        {
            functions = ImmutableListMultimap.of();
        }

        public FunctionMap(FunctionMap map, Iterable<? extends ParametricFunction> functions)
        {
            this.functions = ImmutableListMultimap.<QualifiedName, ParametricFunction>builder()
                    .putAll(map.functions)
                    .putAll(Multimaps.index(functions, function -> QualifiedName.of(function.getSignature().getName())))
                    .build();

            // Make sure all functions with the same name are aggregations or none of them are
            for (Map.Entry<QualifiedName, Collection<ParametricFunction>> entry : this.functions.asMap().entrySet()) {
                Collection<ParametricFunction> values = entry.getValue();
                checkState(Iterables.all(values, ParametricFunction::isAggregate) || !Iterables.any(values, ParametricFunction::isAggregate),
                        "'%s' is both an aggregation and a scalar function", entry.getKey());
            }
        }

        public List<ParametricFunction> list()
        {
            return ImmutableList.copyOf(functions.values());
        }

        public Collection<ParametricFunction> get(QualifiedName name)
        {
            return functions.get(name);
        }
    }
}
