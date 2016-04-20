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

import com.facebook.presto.block.BlockSerdeUtil;
import com.facebook.presto.operator.aggregation.ApproximateAverageAggregations;
import com.facebook.presto.operator.aggregation.ApproximateCountAggregation;
import com.facebook.presto.operator.aggregation.ApproximateCountColumnAggregations;
import com.facebook.presto.operator.aggregation.ApproximateCountDistinctAggregations;
import com.facebook.presto.operator.aggregation.ApproximateDoublePercentileAggregations;
import com.facebook.presto.operator.aggregation.ApproximateDoublePercentileArrayAggregations;
import com.facebook.presto.operator.aggregation.ApproximateLongPercentileAggregations;
import com.facebook.presto.operator.aggregation.ApproximateLongPercentileArrayAggregations;
import com.facebook.presto.operator.aggregation.ApproximateSetAggregation;
import com.facebook.presto.operator.aggregation.ApproximateSumAggregations;
import com.facebook.presto.operator.aggregation.AverageAggregations;
import com.facebook.presto.operator.aggregation.BooleanAndAggregation;
import com.facebook.presto.operator.aggregation.BooleanOrAggregation;
import com.facebook.presto.operator.aggregation.CorrelationAggregation;
import com.facebook.presto.operator.aggregation.CountAggregation;
import com.facebook.presto.operator.aggregation.CountIfAggregation;
import com.facebook.presto.operator.aggregation.CovarianceAggregation;
import com.facebook.presto.operator.aggregation.DoubleSumAggregation;
import com.facebook.presto.operator.aggregation.GeometricMeanAggregations;
import com.facebook.presto.operator.aggregation.InternalAggregationFunction;
import com.facebook.presto.operator.aggregation.LongSumAggregation;
import com.facebook.presto.operator.aggregation.MergeHyperLogLogAggregation;
import com.facebook.presto.operator.aggregation.NumericHistogramAggregation;
import com.facebook.presto.operator.aggregation.RegressionAggregation;
import com.facebook.presto.operator.aggregation.VarianceAggregation;
import com.facebook.presto.operator.scalar.ArrayConcatFunction;
import com.facebook.presto.operator.scalar.ArrayDistinctFunction;
import com.facebook.presto.operator.scalar.ArrayElementAtFunction;
import com.facebook.presto.operator.scalar.ArrayEqualOperator;
import com.facebook.presto.operator.scalar.ArrayFunctions;
import com.facebook.presto.operator.scalar.ArrayGreaterThanOperator;
import com.facebook.presto.operator.scalar.ArrayGreaterThanOrEqualOperator;
import com.facebook.presto.operator.scalar.ArrayHashCodeOperator;
import com.facebook.presto.operator.scalar.ArrayLessThanOrEqualOperator;
import com.facebook.presto.operator.scalar.ArrayMaxFunction;
import com.facebook.presto.operator.scalar.ArrayMinFunction;
import com.facebook.presto.operator.scalar.ArrayNotEqualOperator;
import com.facebook.presto.operator.scalar.ArrayRemoveFunction;
import com.facebook.presto.operator.scalar.ArraySliceFunction;
import com.facebook.presto.operator.scalar.BitwiseFunctions;
import com.facebook.presto.operator.scalar.ColorFunctions;
import com.facebook.presto.operator.scalar.CombineHashFunction;
import com.facebook.presto.operator.scalar.DateTimeFunctions;
import com.facebook.presto.operator.scalar.FailureFunction;
import com.facebook.presto.operator.scalar.HyperLogLogFunctions;
import com.facebook.presto.operator.scalar.JsonFunctions;
import com.facebook.presto.operator.scalar.JsonOperators;
import com.facebook.presto.operator.scalar.MapCardinalityFunction;
import com.facebook.presto.operator.scalar.MapConcatFunction;
import com.facebook.presto.operator.scalar.MapEqualOperator;
import com.facebook.presto.operator.scalar.MapKeys;
import com.facebook.presto.operator.scalar.MapNotEqualOperator;
import com.facebook.presto.operator.scalar.MapToMapCast;
import com.facebook.presto.operator.scalar.MapValues;
import com.facebook.presto.operator.scalar.MathFunctions;
import com.facebook.presto.operator.scalar.RegexpFunctions;
import com.facebook.presto.operator.scalar.ScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.StringFunctions;
import com.facebook.presto.operator.scalar.UrlFunctions;
import com.facebook.presto.operator.scalar.VarbinaryFunctions;
import com.facebook.presto.operator.window.CumulativeDistributionFunction;
import com.facebook.presto.operator.window.DenseRankFunction;
import com.facebook.presto.operator.window.FirstValueFunction;
import com.facebook.presto.operator.window.LagFunction;
import com.facebook.presto.operator.window.LastValueFunction;
import com.facebook.presto.operator.window.LeadFunction;
import com.facebook.presto.operator.window.NTileFunction;
import com.facebook.presto.operator.window.NthValueFunction;
import com.facebook.presto.operator.window.PercentRankFunction;
import com.facebook.presto.operator.window.RankFunction;
import com.facebook.presto.operator.window.RowNumberFunction;
import com.facebook.presto.operator.window.SqlWindowFunction;
import com.facebook.presto.operator.window.WindowFunctionSupplier;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockEncodingSerde;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.type.BigintOperators;
import com.facebook.presto.type.BooleanOperators;
import com.facebook.presto.type.ColorOperators;
import com.facebook.presto.type.DateOperators;
import com.facebook.presto.type.DateTimeOperators;
import com.facebook.presto.type.DecimalOperators;
import com.facebook.presto.type.DoubleOperators;
import com.facebook.presto.type.HyperLogLogOperators;
import com.facebook.presto.type.IntegerOperators;
import com.facebook.presto.type.IntervalDayTimeOperators;
import com.facebook.presto.type.IntervalYearMonthOperators;
import com.facebook.presto.type.LikeFunctions;
import com.facebook.presto.type.RowParametricType;
import com.facebook.presto.type.TimeOperators;
import com.facebook.presto.type.TimeWithTimeZoneOperators;
import com.facebook.presto.type.TimestampOperators;
import com.facebook.presto.type.TimestampWithTimeZoneOperators;
import com.facebook.presto.type.UnknownOperators;
import com.facebook.presto.type.VarbinaryOperators;
import com.facebook.presto.type.VarcharOperators;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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

import javax.annotation.Nullable;
import javax.annotation.concurrent.ThreadSafe;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.metadata.FunctionKind.APPROXIMATE_AGGREGATE;
import static com.facebook.presto.metadata.FunctionKind.SCALAR;
import static com.facebook.presto.metadata.FunctionKind.WINDOW;
import static com.facebook.presto.metadata.Signature.internalOperator;
import static com.facebook.presto.operator.aggregation.ArbitraryAggregationFunction.ARBITRARY_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ArrayAggregationFunction.ARRAY_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ChecksumAggregationFunction.CHECKSUM_AGGREGATION;
import static com.facebook.presto.operator.aggregation.CountColumn.COUNT_COLUMN;
import static com.facebook.presto.operator.aggregation.Histogram.HISTOGRAM;
import static com.facebook.presto.operator.aggregation.MapAggregationFunction.MAP_AGG;
import static com.facebook.presto.operator.aggregation.MaxAggregationFunction.MAX_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MaxBy.MAX_BY;
import static com.facebook.presto.operator.aggregation.MaxByNAggregationFunction.MAX_BY_N_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MaxNAggregationFunction.MAX_N_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MinAggregationFunction.MIN_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MinBy.MIN_BY;
import static com.facebook.presto.operator.aggregation.MinByNAggregationFunction.MIN_BY_N_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MinNAggregationFunction.MIN_N_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MultimapAggregationFunction.MULTIMAP_AGG;
import static com.facebook.presto.operator.scalar.ArrayCardinalityFunction.ARRAY_CARDINALITY;
import static com.facebook.presto.operator.scalar.ArrayConstructor.ARRAY_CONSTRUCTOR;
import static com.facebook.presto.operator.scalar.ArrayContains.ARRAY_CONTAINS;
import static com.facebook.presto.operator.scalar.ArrayIntersectFunction.ARRAY_INTERSECT_FUNCTION;
import static com.facebook.presto.operator.scalar.ArrayJoin.ARRAY_JOIN;
import static com.facebook.presto.operator.scalar.ArrayJoin.ARRAY_JOIN_WITH_NULL_REPLACEMENT;
import static com.facebook.presto.operator.scalar.ArrayLessThanOperator.ARRAY_LESS_THAN;
import static com.facebook.presto.operator.scalar.ArrayPositionFunction.ARRAY_POSITION;
import static com.facebook.presto.operator.scalar.ArraySortFunction.ARRAY_SORT_FUNCTION;
import static com.facebook.presto.operator.scalar.ArraySubscriptOperator.ARRAY_SUBSCRIPT;
import static com.facebook.presto.operator.scalar.ArrayToArrayCast.ARRAY_TO_ARRAY_CAST;
import static com.facebook.presto.operator.scalar.ArrayToElementConcatFunction.ARRAY_TO_ELEMENT_CONCAT_FUNCTION;
import static com.facebook.presto.operator.scalar.ArrayToJsonCast.ARRAY_TO_JSON;
import static com.facebook.presto.operator.scalar.CastFromUnknownOperator.CAST_FROM_UNKNOWN;
import static com.facebook.presto.operator.scalar.ConcatFunction.CONCAT;
import static com.facebook.presto.operator.scalar.ElementToArrayConcatFunction.ELEMENT_TO_ARRAY_CONCAT_FUNCTION;
import static com.facebook.presto.operator.scalar.Greatest.GREATEST;
import static com.facebook.presto.operator.scalar.IdentityCast.IDENTITY_CAST;
import static com.facebook.presto.operator.scalar.JsonToArrayCast.JSON_TO_ARRAY;
import static com.facebook.presto.operator.scalar.JsonToMapCast.JSON_TO_MAP;
import static com.facebook.presto.operator.scalar.Least.LEAST;
import static com.facebook.presto.operator.scalar.MapConstructor.MAP_CONSTRUCTOR;
import static com.facebook.presto.operator.scalar.MapHashCodeOperator.MAP_HASH_CODE;
import static com.facebook.presto.operator.scalar.MapSubscriptOperator.MAP_SUBSCRIPT;
import static com.facebook.presto.operator.scalar.MapToJsonCast.MAP_TO_JSON;
import static com.facebook.presto.operator.scalar.RowEqualOperator.ROW_EQUAL;
import static com.facebook.presto.operator.scalar.RowHashCodeOperator.ROW_HASH_CODE;
import static com.facebook.presto.operator.scalar.RowNotEqualOperator.ROW_NOT_EQUAL;
import static com.facebook.presto.operator.scalar.RowToJsonCast.ROW_TO_JSON;
import static com.facebook.presto.operator.scalar.TryCastFunction.TRY_CAST;
import static com.facebook.presto.operator.scalar.VarcharToVarcharCast.VARCHAR_TO_VARCHAR_CAST;
import static com.facebook.presto.operator.window.AggregateWindowFunction.supplier;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.type.DecimalCasts.BIGINT_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.BOOLEAN_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_BIGINT_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_BOOLEAN_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_DOUBLE_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_INTEGER_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_VARCHAR_CAST;
import static com.facebook.presto.type.DecimalCasts.DOUBLE_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.INTEGER_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.VARCHAR_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_BETWEEN_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_EQUAL_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_GREATER_THAN_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_LESS_THAN_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_NOT_EQUAL_OPERATOR;
import static com.facebook.presto.type.DecimalOperators.DECIMAL_DIVIDE_OPERATOR;
import static com.facebook.presto.type.DecimalOperators.DECIMAL_MODULUS_OPERATOR;
import static com.facebook.presto.type.DecimalOperators.DECIMAL_MULTIPLY_OPERATOR;
import static com.facebook.presto.type.DecimalToDecimalCasts.DECIMAL_TO_DECIMAL_CAST;
import static com.facebook.presto.type.TypeUtils.resolveTypes;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableList;
import static com.facebook.presto.util.ImmutableCollectors.toImmutableSet;
import static com.facebook.presto.util.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

@ThreadSafe
public class FunctionRegistry
{
    private static final String MAGIC_LITERAL_FUNCTION_PREFIX = "$literal$";
    private static final String OPERATOR_PREFIX = "$operator$";

    // hack: java classes for types that can be used with magic literals
    private static final Set<Class<?>> SUPPORTED_LITERAL_TYPES = ImmutableSet.<Class<?>>of(long.class, double.class, Slice.class, boolean.class);

    private final TypeManager typeManager;
    private final BlockEncodingSerde blockEncodingSerde;
    private final LoadingCache<SpecializedFunctionKey, ScalarFunctionImplementation> specializedScalarCache;
    private final LoadingCache<SpecializedFunctionKey, InternalAggregationFunction> specializedAggregationCache;
    private final LoadingCache<SpecializedFunctionKey, WindowFunctionSupplier> specializedWindowCache;
    private volatile FunctionMap functions = new FunctionMap();

    public FunctionRegistry(TypeManager typeManager, BlockEncodingSerde blockEncodingSerde, boolean experimentalSyntaxEnabled)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
        this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");

        specializedScalarCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(new CacheLoader<SpecializedFunctionKey, ScalarFunctionImplementation>()
                {
                    @Override
                    public ScalarFunctionImplementation load(SpecializedFunctionKey key)
                            throws Exception
                    {
                        // TODO the function map should be updated, so that this cast can be removed
                        SqlScalarFunction scalarFunction = checkType(key.getFunction(), SqlScalarFunction.class, "function");
                        return scalarFunction.specialize(key.getBoundVariables(), key.getArity(), typeManager, FunctionRegistry.this);
                    }
                });

        specializedAggregationCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(new CacheLoader<SpecializedFunctionKey, InternalAggregationFunction>()
                {
                    @Override
                    public InternalAggregationFunction load(SpecializedFunctionKey key)
                            throws Exception
                    {
                        SqlAggregationFunction aggregationFunction = checkType(key.getFunction(), SqlAggregationFunction.class, "function");
                        return aggregationFunction.specialize(key.getBoundVariables(), key.getArity(), typeManager, FunctionRegistry.this);
                    }
                });

        specializedWindowCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(new CacheLoader<SpecializedFunctionKey, WindowFunctionSupplier>()
                {
                    @Override
                    public WindowFunctionSupplier load(SpecializedFunctionKey key)
                            throws Exception
                    {
                        if (key.getFunction() instanceof SqlAggregationFunction) {
                            SqlAggregationFunction aggregationFunction = checkType(key.getFunction(), SqlAggregationFunction.class, "function");
                            return supplier(aggregationFunction.getSignature(), specializedAggregationCache.getUnchecked(key));
                        }
                        else {
                            SqlWindowFunction windowFunction = checkType(key.getFunction(), SqlWindowFunction.class, "function");
                            return windowFunction.specialize(key.getBoundVariables(), key.getArity(), typeManager, FunctionRegistry.this);
                        }
                    }
                });

        FunctionListBuilder builder = new FunctionListBuilder(typeManager)
                .window("row_number", BIGINT, ImmutableList.<Type>of(), RowNumberFunction.class)
                .window("rank", BIGINT, ImmutableList.<Type>of(), RankFunction.class)
                .window("dense_rank", BIGINT, ImmutableList.<Type>of(), DenseRankFunction.class)
                .window("percent_rank", DOUBLE, ImmutableList.<Type>of(), PercentRankFunction.class)
                .window("cume_dist", DOUBLE, ImmutableList.<Type>of(), CumulativeDistributionFunction.class)
                .window("ntile", BIGINT, ImmutableList.<Type>of(BIGINT), NTileFunction.class)
                .window("first_value", FirstValueFunction.class, "T", "T")
                .window("last_value", LastValueFunction.class, "T", "T")
                .window("nth_value", NthValueFunction.class, "T", "T", "bigint")
                .window("lag", LagFunction.class, "T", "T")
                .window("lag", LagFunction.class, "T", "T", "bigint")
                .window("lag", LagFunction.class, "T", "T", "bigint", "T")
                .window("lead", LeadFunction.class, "T", "T")
                .window("lead", LeadFunction.class, "T", "T", "bigint")
                .window("lead", LeadFunction.class, "T", "T", "bigint", "T")
                .aggregate(CountAggregation.class)
                .aggregate(VarianceAggregation.class)
                .aggregate(ApproximateLongPercentileAggregations.class)
                .aggregate(ApproximateLongPercentileArrayAggregations.class)
                .aggregate(ApproximateDoublePercentileAggregations.class)
                .aggregate(ApproximateDoublePercentileArrayAggregations.class)
                .aggregate(CountIfAggregation.class)
                .aggregate(BooleanAndAggregation.class)
                .aggregate(BooleanOrAggregation.class)
                .aggregate(DoubleSumAggregation.class)
                .aggregate(LongSumAggregation.class)
                .aggregate(AverageAggregations.class)
                .aggregate(GeometricMeanAggregations.class)
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
                .scalar(BitwiseFunctions.class)
                .scalar(DateTimeFunctions.class)
                .scalar(JsonFunctions.class)
                .scalar(ColorFunctions.class)
                .scalar(ColorOperators.class)
                .scalar(HyperLogLogFunctions.class)
                .scalar(UnknownOperators.class)
                .scalar(BooleanOperators.class)
                .scalar(BigintOperators.class)
                .scalar(IntegerOperators.class)
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
                .scalar(FailureFunction.class)
                .scalar(DecimalOperators.Add.class)
                .scalar(DecimalOperators.Subtract.class)
                .scalar(DecimalOperators.Negation.class)
                .functions(IDENTITY_CAST, CAST_FROM_UNKNOWN)
                .scalar(ArrayLessThanOrEqualOperator.class)
                .scalar(ArrayRemoveFunction.class)
                .scalar(ArrayGreaterThanOperator.class)
                .scalar(ArrayGreaterThanOrEqualOperator.class)
                .scalar(ArrayElementAtFunction.class)
                .scalar(ArrayMinFunction.class)
                .scalar(ArrayMaxFunction.class)
                .scalar(ArrayDistinctFunction.class)
                .scalar(ArrayConcatFunction.class)
                .scalar(ArrayNotEqualOperator.class)
                .scalar(ArrayEqualOperator.class)
                .scalar(ArrayHashCodeOperator.class)
                .scalar(ArraySliceFunction.class)
                .scalar(MapEqualOperator.class)
                .scalar(MapNotEqualOperator.class)
                .scalar(MapKeys.class)
                .scalar(MapValues.class)
                .scalar(MapCardinalityFunction.class)
                .scalar(MapConcatFunction.class)
                .scalar(MapToMapCast.class)
                .functions(ARRAY_CONTAINS, ARRAY_JOIN, ARRAY_JOIN_WITH_NULL_REPLACEMENT)
                .functions(ARRAY_TO_ARRAY_CAST, ARRAY_LESS_THAN)
                .functions(ARRAY_TO_ELEMENT_CONCAT_FUNCTION, ELEMENT_TO_ARRAY_CONCAT_FUNCTION)
                .function(MAP_HASH_CODE)
                .functions(ARRAY_CONSTRUCTOR, ARRAY_SUBSCRIPT, ARRAY_CARDINALITY, ARRAY_POSITION, ARRAY_SORT_FUNCTION, ARRAY_INTERSECT_FUNCTION, ARRAY_TO_JSON, JSON_TO_ARRAY)
                .functions(MAP_CONSTRUCTOR, MAP_SUBSCRIPT, MAP_TO_JSON, JSON_TO_MAP)
                .functions(MAP_AGG, MULTIMAP_AGG)
                .functions(DECIMAL_TO_VARCHAR_CAST, DECIMAL_TO_INTEGER_CAST, DECIMAL_TO_BIGINT_CAST, DECIMAL_TO_DOUBLE_CAST, DECIMAL_TO_BOOLEAN_CAST)
                .functions(VARCHAR_TO_DECIMAL_CAST, INTEGER_TO_DECIMAL_CAST, BIGINT_TO_DECIMAL_CAST, DOUBLE_TO_DECIMAL_CAST, BOOLEAN_TO_DECIMAL_CAST)
                .functions(DECIMAL_MULTIPLY_OPERATOR, DECIMAL_DIVIDE_OPERATOR, DECIMAL_MODULUS_OPERATOR)
                .functions(DECIMAL_EQUAL_OPERATOR, DECIMAL_NOT_EQUAL_OPERATOR)
                .functions(DECIMAL_LESS_THAN_OPERATOR, DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR)
                .functions(DECIMAL_GREATER_THAN_OPERATOR, DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR)
                .function(DECIMAL_BETWEEN_OPERATOR)
                .function(HISTOGRAM)
                .function(CHECKSUM_AGGREGATION)
                .function(VARCHAR_TO_VARCHAR_CAST)
                .function(IDENTITY_CAST)
                .function(ARBITRARY_AGGREGATION)
                .function(ARRAY_AGGREGATION)
                .functions(GREATEST, LEAST)
                .functions(MAX_BY, MIN_BY, MAX_BY_N_AGGREGATION, MIN_BY_N_AGGREGATION)
                .functions(MAX_AGGREGATION, MIN_AGGREGATION, MAX_N_AGGREGATION, MIN_N_AGGREGATION)
                .function(COUNT_COLUMN)
                .functions(ROW_HASH_CODE, ROW_TO_JSON, ROW_EQUAL, ROW_NOT_EQUAL)
                .function(CONCAT)
                .function(DECIMAL_TO_DECIMAL_CAST)
                .function(TRY_CAST);

        if (experimentalSyntaxEnabled) {
            builder.aggregate(ApproximateAverageAggregations.class)
                    .aggregate(ApproximateSumAggregations.class)
                    .aggregate(ApproximateCountAggregation.class)
                    .aggregate(ApproximateCountColumnAggregations.class);
        }

        addFunctions(builder.getFunctions());
    }

    public static Signature bindSignature(Signature signature, Map<String, Type> boundTypeVariables, int arity)
    {
        checkArgument((signature.isVariableArity() && arity >= signature.getArgumentTypes().size() - 1) || arity == signature.getArgumentTypes().size(),
                "Illegal arity %d for function %s", arity, signature);
        List<TypeSignature> argumentTypes = signature.getArgumentTypes();
        ImmutableList.Builder<TypeSignature> boundArguments = ImmutableList.builder();
        for (int i = 0; i < argumentTypes.size() - 1; i++) {
            boundArguments.add(argumentTypes.get(i).bindParameters(boundTypeVariables));
        }
        if (!argumentTypes.isEmpty()) {
            TypeSignature lastArgument = argumentTypes.get(argumentTypes.size() - 1).bindParameters(boundTypeVariables);
            if (signature.isVariableArity()) {
                for (int i = 0; i < arity - (argumentTypes.size() - 1); i++) {
                    boundArguments.add(lastArgument);
                }
            }
            else {
                boundArguments.add(lastArgument);
            }
        }
        return new Signature(
                signature.getName(),
                signature.getKind(),
                ImmutableList.of(),
                signature.getLongVariableConstraints(), // these are not bound by this method; refactored in future patch which is in flight.
                signature.getReturnType().bindParameters(boundTypeVariables),
                boundArguments.build(),
                false);
    }

    public final synchronized void addFunctions(List<? extends SqlFunction> functions)
    {
        for (SqlFunction function : functions) {
            for (SqlFunction existingFunction : this.functions.list()) {
                checkArgument(!function.getSignature().equals(existingFunction.getSignature()), "Function already registered: %s", function.getSignature());
            }
        }
        this.functions = new FunctionMap(this.functions, functions);
    }

    public List<SqlFunction> list()
    {
        return functions.list().stream()
                .filter(function -> !function.isHidden())
                .collect(toImmutableList());
    }

    public boolean isAggregationFunction(QualifiedName name)
    {
        return Iterables.any(functions.get(name), function -> function.getSignature().getKind() == AGGREGATE || function.getSignature().getKind() == APPROXIMATE_AGGREGATE);
    }

    public Signature resolveFunction(QualifiedName name, List<TypeSignature> parameterTypes, boolean approximate)
    {
        List<SqlFunction> candidates = functions.get(name).stream()
                .filter(function -> function.getSignature().getKind() == SCALAR || (function.getSignature().getKind() == APPROXIMATE_AGGREGATE) == approximate)
                .collect(toImmutableList());

        List<Type> resolvedTypes = resolveTypes(parameterTypes, typeManager);
        // search for exact match
        Signature match = null;
        for (SqlFunction function : candidates) {
            Map<String, Type> boundParameters = function.getSignature().bindTypeVariables(resolvedTypes, false, typeManager);
            if (boundParameters == null) {
                continue;
            }
            Signature signature = bindSignature(function.getSignature(), boundParameters, resolvedTypes.size());
            if (signature != null) {
                checkArgument(match == null, "Ambiguous call to %s with parameters %s", name, parameterTypes);
                match = signature;
            }
        }

        if (match != null) {
            return match.resolveCalculatedTypes(parameterTypes);
        }

        // search for coerced matches
        List<SqlFunction> coercedCandidates = new ArrayList<>();
        Signature firstCoercedMatch = null;
        for (SqlFunction function : candidates) {
            Map<String, Type> boundTypeVariables = function.getSignature().bindTypeVariables(resolvedTypes, true, typeManager);
            if (boundTypeVariables == null) {
                continue;
            }
            Signature signature = bindSignature(function.getSignature(), boundTypeVariables, resolvedTypes.size());
            if (signature != null) {
                coercedCandidates.add(function);

                if (firstCoercedMatch == null) {
                    firstCoercedMatch = signature;
                }
            }
        }

        // search for a 'best' coerced match if it exists
        // TODO: remove when we move to a lattice-based type coercion system
        // TODO: this is a hack that relies on the fact that all functions are specified for bigints, but not for the narrower integral types
        // converts any ints to bigints and then see if there is an exact match
        List<Type> promotedTypes = resolvedTypes.stream()
                .map(type -> type == INTEGER ? BIGINT : type)
                .collect(Collectors.toList());

        for (SqlFunction coercedFunction : coercedCandidates) {
            Map<String, Type> boundTypeVariables = coercedFunction.getSignature().bindTypeVariables(promotedTypes, false, typeManager);
            if (boundTypeVariables == null) {
                continue;
            }
            Signature signature = bindSignature(coercedFunction.getSignature(), boundTypeVariables, resolvedTypes.size());
            if (signature != null) {
                checkState(match == null, "ambiguous function implementations found when integers were cast to bigints");
                match = signature;
            }
        }

        if (match == null || coercedCandidates.size() == 1) {
            // i.e. revert to old behavior
            match = firstCoercedMatch; // TODO: this does not deal with ambiguities
        }

        if (match != null) {
            return match.resolveCalculatedTypes(parameterTypes);
        }

        List<String> expectedParameters = new ArrayList<>();
        for (SqlFunction function : candidates) {
            expectedParameters.add(format("%s(%s) %s",
                    name,
                    Joiner.on(", ").join(function.getSignature().getArgumentTypes()),
                    Joiner.on(", ").join(function.getSignature().getTypeVariableConstraints())));
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
            requireNonNull(type, format("Type %s not registered", typeName));

            // verify we have one parameter of the proper type
            checkArgument(parameterTypes.size() == 1, "Expected one argument to literal function, but got %s", parameterTypes);
            Type parameterType = typeManager.getType(parameterTypes.get(0));
            requireNonNull(parameterType, format("Type %s not found", parameterTypes.get(0)));

            return getMagicLiteralFunctionSignature(type);
        }

        // TODO this should be removed and implemented as a special expression type
        if (parameterTypes.size() == 1 && parameterTypes.get(0).getBase().equals(StandardTypes.ROW)) {
            SqlFunction fieldReference = getRowFieldReference(name.getSuffix(), parameterTypes.get(0));
            if (fieldReference != null) {
                Map<String, Type> boundParameters = fieldReference.getSignature().bindTypeVariables(resolvedTypes, true, typeManager);
                if (boundParameters != null) {
                    return bindSignature(fieldReference.getSignature(), boundParameters, resolvedTypes.size());
                }
            }
        }

        throw new PrestoException(FUNCTION_NOT_FOUND, message);
    }

    @Nullable
    private SqlFunction getRowFieldReference(String field, TypeSignature rowTypeSignature)
    {
        Type rowType = typeManager.getType(rowTypeSignature);
        checkState(rowType.getTypeSignature().getBase().equals(StandardTypes.ROW), "rowType is not a ROW type");
        SqlFunction match = null;
        for (SqlFunction function : RowParametricType.ROW.createFunctions(rowType)) {
            if (!function.getSignature().getName().equalsIgnoreCase(field)) {
                continue;
            }
            checkArgument(match == null, "Ambiguous field %s in type %s", field, rowType.getDisplayName());
            match = function;
        }

        return match;
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(Signature signature)
    {
        checkArgument(signature.getKind() == WINDOW || signature.getKind() == AGGREGATE, "%s is not a window function", signature);
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);
        Iterable<SqlFunction> candidates = functions.get(QualifiedName.of(signature.getName()));
        // search for exact match
        for (SqlFunction operator : candidates) {
            Type returnType = typeManager.getType(signature.getReturnType());
            List<Type> argumentTypes = resolveTypes(signature.getArgumentTypes(), typeManager);
            Map<String, Type> boundTypeParameters = operator.getSignature().bindTypeVariables(returnType, argumentTypes, false, typeManager);
            if (boundTypeParameters != null) {
                try {
                    Map<String, OptionalLong> boundLiteralParameters = operator.getSignature().bindLongVariables(signature.getArgumentTypes());
                    BoundVariables boundVariables = new BoundVariables(boundTypeParameters, boundLiteralParameters);
                    return specializedWindowCache.getUnchecked(new SpecializedFunctionKey(operator, boundVariables, argumentTypes.size()));
                }
                catch (UncheckedExecutionException e) {
                    throw Throwables.propagate(e.getCause());
                }
            }
        }
        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("%s not found", signature));
    }

    public InternalAggregationFunction getAggregateFunctionImplementation(Signature signature)
    {
        checkArgument(signature.getKind() == AGGREGATE || signature.getKind() == APPROXIMATE_AGGREGATE, "%s is not an aggregate function", signature);
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);
        Iterable<SqlFunction> candidates = functions.get(QualifiedName.of(signature.getName()));
        // search for exact match
        for (SqlFunction operator : candidates) {
            Type returnType = typeManager.getType(signature.getReturnType());
            List<Type> argumentTypes = resolveTypes(signature.getArgumentTypes(), typeManager);
            Map<String, Type> boundTypeParameters = operator.getSignature().bindTypeVariables(returnType, argumentTypes, false, typeManager);
            if (boundTypeParameters != null) {
                try {
                    Map<String, OptionalLong> boundLiteralParameters = operator.getSignature().bindLongVariables(signature.getArgumentTypes());
                    BoundVariables boundVariables = new BoundVariables(boundTypeParameters, boundLiteralParameters);
                    return specializedAggregationCache.getUnchecked(new SpecializedFunctionKey(operator, boundVariables, signature.getArgumentTypes().size()));
                }
                catch (UncheckedExecutionException e) {
                    throw Throwables.propagate(e.getCause());
                }
            }
        }
        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("%s not found", signature));
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(Signature signature)
    {
        checkArgument(signature.getKind() == SCALAR, "%s is not a scalar function", signature);
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);
        Iterable<SqlFunction> candidates = functions.get(QualifiedName.of(signature.getName()));
        // search for exact match
        Type returnType = typeManager.getType(signature.getReturnType());
        List<Type> argumentTypes = resolveTypes(signature.getArgumentTypes(), typeManager);
        for (SqlFunction operator : candidates) {
            Map<String, Type> boundTypeParameters = operator.getSignature().bindTypeVariables(returnType, argumentTypes, false, typeManager);
            if (boundTypeParameters != null) {
                try {
                    Map<String, OptionalLong> boundLiteralParameters = operator.getSignature().bindLongVariables(signature.getArgumentTypes());
                    BoundVariables boundVariables = new BoundVariables(boundTypeParameters, boundLiteralParameters);
                    return specializedScalarCache.getUnchecked(new SpecializedFunctionKey(operator, boundVariables, argumentTypes.size()));
                }
                catch (UncheckedExecutionException e) {
                    throw Throwables.propagate(e.getCause());
                }
            }
        }

        // TODO: this is a hack and should be removed
        if (signature.getName().startsWith(MAGIC_LITERAL_FUNCTION_PREFIX)) {
            List<TypeSignature> parameterTypes = signature.getArgumentTypes();
            // extract type from function name
            String typeName = signature.getName().substring(MAGIC_LITERAL_FUNCTION_PREFIX.length());

            // lookup the type
            Type type = typeManager.getType(parseTypeSignature(typeName));
            requireNonNull(type, format("Type %s not registered", typeName));

            // verify we have one parameter of the proper type
            checkArgument(parameterTypes.size() == 1, "Expected one argument to literal function, but got %s", parameterTypes);
            Type parameterType = typeManager.getType(parameterTypes.get(0));
            requireNonNull(parameterType, format("Type %s not found", parameterTypes.get(0)));

            MethodHandle methodHandle = null;
            if (parameterType.getJavaType() == type.getJavaType()) {
                methodHandle = MethodHandles.identity(parameterType.getJavaType());
            }

            if (parameterType.getJavaType() == Slice.class) {
                if (type.getJavaType() == Block.class) {
                    methodHandle = BlockSerdeUtil.READ_BLOCK.bindTo(blockEncodingSerde);
                }
            }

            checkArgument(methodHandle != null,
                    "Expected type %s to use (or can be converted into) Java type %s, but Java type is %s",
                    type,
                    parameterType.getJavaType(),
                    type.getJavaType());

            return new ScalarFunctionImplementation(false, ImmutableList.of(false), methodHandle, true);
        }

        // TODO this should be removed and implemented as a special expression type
        if (!signature.getArgumentTypes().isEmpty() && signature.getArgumentTypes().get(0).getBase().equals(StandardTypes.ROW)) {
            SqlFunction fieldReference = getRowFieldReference(signature.getName(), signature.getArgumentTypes().get(0));
            if (fieldReference != null) {
                Map<String, Type> boundTypeParameters = fieldReference.getSignature().bindTypeVariables(returnType, argumentTypes, false, typeManager);
                Map<String, OptionalLong> boundLiteralParameters = fieldReference.getSignature().bindLongVariables(signature.getArgumentTypes());
                BoundVariables boundVariables = new BoundVariables(boundTypeParameters, boundLiteralParameters);
                return specializedScalarCache.getUnchecked(new SpecializedFunctionKey(fieldReference, boundVariables, argumentTypes.size()));
            }
        }
        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("%s not found", signature));
    }

    @VisibleForTesting
    public List<SqlFunction> listOperators()
    {
        Set<String> operatorNames = Arrays.asList(OperatorType.values()).stream()
                .map(FunctionRegistry::mangleOperatorName)
                .collect(toImmutableSet());

        return functions.list().stream()
                .filter(function -> operatorNames.contains(function.getSignature().getName()))
                .collect(toImmutableList());
    }

    public boolean canResolveOperator(OperatorType operatorType, Type returnType, List<? extends Type> argumentTypes)
    {
        Signature signature = internalOperator(operatorType, returnType, argumentTypes);
        return isRegistered(signature);
    }

    public boolean isRegistered(Signature signature)
    {
        try {
            // TODO: this is hacky, but until the magic literal and row field reference hacks are cleaned up it's difficult to implement this.
            getScalarFunctionImplementation(signature);
            return true;
        }
        catch (PrestoException e) {
            if (e.getErrorCode().getCode() == FUNCTION_IMPLEMENTATION_MISSING.toErrorCode().getCode()) {
                return false;
            }
            throw e;
        }
    }

    public Signature resolveOperator(OperatorType operatorType, List<? extends Type> argumentTypes)
            throws OperatorNotFoundException
    {
        try {
            return resolveFunction(QualifiedName.of(mangleOperatorName(operatorType)), Lists.transform(argumentTypes, Type::getTypeSignature), false);
        }
        catch (PrestoException e) {
            if (e.getErrorCode().getCode() == FUNCTION_NOT_FOUND.toErrorCode().getCode()) {
                throw new OperatorNotFoundException(
                        operatorType,
                        argumentTypes.stream()
                                .map(Type::getTypeSignature)
                                .collect(toImmutableList()));
            }
            else {
                throw e;
            }
        }
    }

    public Signature getCoercion(Type fromType, Type toType)
    {
        return getCoercion(fromType.getTypeSignature(), toType.getTypeSignature());
    }

    public Signature getCoercion(TypeSignature fromType, TypeSignature toType)
    {
        Signature signature = internalOperator(OperatorType.CAST.name(), toType, ImmutableList.of(fromType));
        try {
            getScalarFunctionImplementation(signature);
        }
        catch (PrestoException e) {
            if (e.getErrorCode().getCode() == FUNCTION_IMPLEMENTATION_MISSING.toErrorCode().getCode()) {
                throw new OperatorNotFoundException(OperatorType.CAST, ImmutableList.of(fromType), toType);
            }
            throw e;
        }
        return signature;
    }

    public static Type typeForMagicLiteral(Type type)
    {
        Class<?> clazz = type.getJavaType();
        clazz = Primitives.unwrap(clazz);

        if (clazz == long.class) {
            return BIGINT;
        }
        if (clazz == double.class) {
            return DOUBLE;
        }
        if (!clazz.isPrimitive()) {
            if (type instanceof VarcharType) {
                return type;
            }
            else {
                return VARBINARY;
            }
        }
        if (clazz == boolean.class) {
            return BOOLEAN;
        }
        throw new IllegalArgumentException("Unhandled Java type: " + clazz.getName());
    }

    public static Signature getMagicLiteralFunctionSignature(Type type)
    {
        TypeSignature argumentType = typeForMagicLiteral(type).getTypeSignature();

        return new Signature(MAGIC_LITERAL_FUNCTION_PREFIX + type.getTypeSignature(),
                SCALAR,
                type.getTypeSignature(),
                argumentType);
    }

    public static boolean isSupportedLiteralType(Type type)
    {
        return SUPPORTED_LITERAL_TYPES.contains(type.getJavaType());
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
        private final Multimap<QualifiedName, SqlFunction> functions;

        public FunctionMap()
        {
            functions = ImmutableListMultimap.of();
        }

        public FunctionMap(FunctionMap map, Iterable<? extends SqlFunction> functions)
        {
            this.functions = ImmutableListMultimap.<QualifiedName, SqlFunction>builder()
                    .putAll(map.functions)
                    .putAll(Multimaps.index(functions, function -> QualifiedName.of(function.getSignature().getName())))
                    .build();

            // Make sure all functions with the same name are aggregations or none of them are
            for (Map.Entry<QualifiedName, Collection<SqlFunction>> entry : this.functions.asMap().entrySet()) {
                Collection<SqlFunction> values = entry.getValue();
                long aggregations = values.stream()
                        .map(function -> function.getSignature().getKind())
                        .filter(kind -> kind == AGGREGATE || kind == APPROXIMATE_AGGREGATE)
                        .count();
                checkState(aggregations == 0 || aggregations == values.size(), "'%s' is both an aggregation and a scalar function", entry.getKey());
            }
        }

        public List<SqlFunction> list()
        {
            return ImmutableList.copyOf(functions.values());
        }

        public Collection<SqlFunction> get(QualifiedName name)
        {
            return functions.get(name);
        }
    }
}
