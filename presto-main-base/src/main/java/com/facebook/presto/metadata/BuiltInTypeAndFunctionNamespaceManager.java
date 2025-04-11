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

import com.facebook.presto.UnknownTypeException;
import com.facebook.presto.common.CatalogSchemaName;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockEncodingSerde;
import com.facebook.presto.common.block.BlockSerdeUtil;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.function.SqlFunctionResult;
import com.facebook.presto.common.type.DistinctTypeInfo;
import com.facebook.presto.common.type.ParametricType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.TypeParameter;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.UserDefinedType;
import com.facebook.presto.expressions.DynamicFilters.DynamicFilterPlaceholderFunction;
import com.facebook.presto.geospatial.BingTileFunctions;
import com.facebook.presto.geospatial.BingTileOperators;
import com.facebook.presto.geospatial.GeoFunctions;
import com.facebook.presto.geospatial.KdbTreeCasts;
import com.facebook.presto.geospatial.SpatialPartitioningAggregateFunction;
import com.facebook.presto.geospatial.SpatialPartitioningInternalAggregateFunction;
import com.facebook.presto.geospatial.SphericalGeoFunctions;
import com.facebook.presto.geospatial.aggregation.ConvexHullAggregation;
import com.facebook.presto.geospatial.aggregation.GeometryUnionAgg;
import com.facebook.presto.operator.aggregation.AlternativeApproxPercentile;
import com.facebook.presto.operator.aggregation.ApproximateCountDistinctAggregation;
import com.facebook.presto.operator.aggregation.ApproximateDoublePercentileAggregations;
import com.facebook.presto.operator.aggregation.ApproximateDoublePercentileArrayAggregations;
import com.facebook.presto.operator.aggregation.ApproximateLongPercentileAggregations;
import com.facebook.presto.operator.aggregation.ApproximateLongPercentileArrayAggregations;
import com.facebook.presto.operator.aggregation.ApproximateRealPercentileAggregations;
import com.facebook.presto.operator.aggregation.ApproximateRealPercentileArrayAggregations;
import com.facebook.presto.operator.aggregation.ApproximateSetAggregation;
import com.facebook.presto.operator.aggregation.AverageAggregations;
import com.facebook.presto.operator.aggregation.BitwiseAndAggregation;
import com.facebook.presto.operator.aggregation.BitwiseOrAggregation;
import com.facebook.presto.operator.aggregation.BitwiseXorAggregation;
import com.facebook.presto.operator.aggregation.BooleanAndAggregation;
import com.facebook.presto.operator.aggregation.BooleanOrAggregation;
import com.facebook.presto.operator.aggregation.CentralMomentsAggregation;
import com.facebook.presto.operator.aggregation.ClassificationFallOutAggregation;
import com.facebook.presto.operator.aggregation.ClassificationMissRateAggregation;
import com.facebook.presto.operator.aggregation.ClassificationPrecisionAggregation;
import com.facebook.presto.operator.aggregation.ClassificationRecallAggregation;
import com.facebook.presto.operator.aggregation.ClassificationThresholdsAggregation;
import com.facebook.presto.operator.aggregation.CountAggregation;
import com.facebook.presto.operator.aggregation.CountIfAggregation;
import com.facebook.presto.operator.aggregation.DefaultApproximateCountDistinctAggregation;
import com.facebook.presto.operator.aggregation.DoubleCorrelationAggregation;
import com.facebook.presto.operator.aggregation.DoubleCovarianceAggregation;
import com.facebook.presto.operator.aggregation.DoubleHistogramAggregation;
import com.facebook.presto.operator.aggregation.DoubleRegressionAggregation;
import com.facebook.presto.operator.aggregation.DoubleSumAggregation;
import com.facebook.presto.operator.aggregation.EntropyAggregation;
import com.facebook.presto.operator.aggregation.GeometricMeanAggregations;
import com.facebook.presto.operator.aggregation.IntervalDayToSecondAverageAggregation;
import com.facebook.presto.operator.aggregation.IntervalDayToSecondSumAggregation;
import com.facebook.presto.operator.aggregation.IntervalYearToMonthAverageAggregation;
import com.facebook.presto.operator.aggregation.IntervalYearToMonthSumAggregation;
import com.facebook.presto.operator.aggregation.LongSumAggregation;
import com.facebook.presto.operator.aggregation.MaxDataSizeForStats;
import com.facebook.presto.operator.aggregation.MergeHyperLogLogAggregation;
import com.facebook.presto.operator.aggregation.MergeQuantileDigestFunction;
import com.facebook.presto.operator.aggregation.MergeTDigestFunction;
import com.facebook.presto.operator.aggregation.RealCorrelationAggregation;
import com.facebook.presto.operator.aggregation.RealCovarianceAggregation;
import com.facebook.presto.operator.aggregation.RealGeometricMeanAggregations;
import com.facebook.presto.operator.aggregation.RealHistogramAggregation;
import com.facebook.presto.operator.aggregation.RealRegressionAggregation;
import com.facebook.presto.operator.aggregation.RealSumAggregation;
import com.facebook.presto.operator.aggregation.ReduceAggregationFunction;
import com.facebook.presto.operator.aggregation.SumDataSizeForStats;
import com.facebook.presto.operator.aggregation.VarianceAggregation;
import com.facebook.presto.operator.aggregation.approxmostfrequent.ApproximateMostFrequent;
import com.facebook.presto.operator.aggregation.arrayagg.ArrayAggregationFunction;
import com.facebook.presto.operator.aggregation.differentialentropy.DifferentialEntropyAggregation;
import com.facebook.presto.operator.aggregation.histogram.Histogram;
import com.facebook.presto.operator.aggregation.multimapagg.AlternativeMultimapAggregationFunction;
import com.facebook.presto.operator.aggregation.multimapagg.MultimapAggregationFunction;
import com.facebook.presto.operator.aggregation.noisyaggregation.NoisyApproximateDistinctCountSfmAggregation;
import com.facebook.presto.operator.aggregation.noisyaggregation.NoisyApproximateDistinctCountSfmAggregationDefaultBucketsPrecision;
import com.facebook.presto.operator.aggregation.noisyaggregation.NoisyApproximateDistinctCountSfmAggregationDefaultPrecision;
import com.facebook.presto.operator.aggregation.noisyaggregation.NoisyApproximateSetSfmAggregation;
import com.facebook.presto.operator.aggregation.noisyaggregation.NoisyApproximateSetSfmAggregationDefaultBucketsPrecision;
import com.facebook.presto.operator.aggregation.noisyaggregation.NoisyApproximateSetSfmAggregationDefaultPrecision;
import com.facebook.presto.operator.aggregation.noisyaggregation.NoisyApproximateSetSfmFromIndexAndZerosAggregation;
import com.facebook.presto.operator.aggregation.noisyaggregation.NoisyApproximateSetSfmFromIndexAndZerosAggregationDefaultPrecision;
import com.facebook.presto.operator.aggregation.noisyaggregation.NoisyCountIfGaussianAggregation;
import com.facebook.presto.operator.aggregation.noisyaggregation.SfmSketchMergeAggregation;
import com.facebook.presto.operator.aggregation.reservoirsample.ReservoirSampleFunction;
import com.facebook.presto.operator.aggregation.sketch.kll.KllSketchAggregationFunction;
import com.facebook.presto.operator.aggregation.sketch.kll.KllSketchWithKAggregationFunction;
import com.facebook.presto.operator.aggregation.sketch.theta.ThetaSketchAggregationFunction;
import com.facebook.presto.operator.scalar.ArrayAllMatchFunction;
import com.facebook.presto.operator.scalar.ArrayAnyMatchFunction;
import com.facebook.presto.operator.scalar.ArrayCardinalityFunction;
import com.facebook.presto.operator.scalar.ArrayCombinationsFunction;
import com.facebook.presto.operator.scalar.ArrayContains;
import com.facebook.presto.operator.scalar.ArrayCumSum;
import com.facebook.presto.operator.scalar.ArrayDistinctFromOperator;
import com.facebook.presto.operator.scalar.ArrayDistinctFunction;
import com.facebook.presto.operator.scalar.ArrayElementAtFunction;
import com.facebook.presto.operator.scalar.ArrayEqualOperator;
import com.facebook.presto.operator.scalar.ArrayExceptFunction;
import com.facebook.presto.operator.scalar.ArrayFilterFunction;
import com.facebook.presto.operator.scalar.ArrayFindFirstFunction;
import com.facebook.presto.operator.scalar.ArrayFindFirstIndexFunction;
import com.facebook.presto.operator.scalar.ArrayFindFirstIndexWithOffsetFunction;
import com.facebook.presto.operator.scalar.ArrayFindFirstWithOffsetFunction;
import com.facebook.presto.operator.scalar.ArrayFunctions;
import com.facebook.presto.operator.scalar.ArrayGreaterThanOperator;
import com.facebook.presto.operator.scalar.ArrayGreaterThanOrEqualOperator;
import com.facebook.presto.operator.scalar.ArrayHashCodeOperator;
import com.facebook.presto.operator.scalar.ArrayIndeterminateOperator;
import com.facebook.presto.operator.scalar.ArrayIntersectFunction;
import com.facebook.presto.operator.scalar.ArrayLessThanOperator;
import com.facebook.presto.operator.scalar.ArrayLessThanOrEqualOperator;
import com.facebook.presto.operator.scalar.ArrayMaxFunction;
import com.facebook.presto.operator.scalar.ArrayMinFunction;
import com.facebook.presto.operator.scalar.ArrayNgramsFunction;
import com.facebook.presto.operator.scalar.ArrayNoneMatchFunction;
import com.facebook.presto.operator.scalar.ArrayNormalizeFunction;
import com.facebook.presto.operator.scalar.ArrayNotEqualOperator;
import com.facebook.presto.operator.scalar.ArrayPositionFunction;
import com.facebook.presto.operator.scalar.ArrayPositionWithIndexFunction;
import com.facebook.presto.operator.scalar.ArrayRemoveFunction;
import com.facebook.presto.operator.scalar.ArrayReverseFunction;
import com.facebook.presto.operator.scalar.ArrayShuffleFunction;
import com.facebook.presto.operator.scalar.ArraySliceFunction;
import com.facebook.presto.operator.scalar.ArraySortComparatorFunction;
import com.facebook.presto.operator.scalar.ArraySortFunction;
import com.facebook.presto.operator.scalar.ArraySumBigIntFunction;
import com.facebook.presto.operator.scalar.ArraySumDoubleFunction;
import com.facebook.presto.operator.scalar.ArrayTrimFunction;
import com.facebook.presto.operator.scalar.ArrayUnionFunction;
import com.facebook.presto.operator.scalar.ArraysOverlapFunction;
import com.facebook.presto.operator.scalar.BitwiseFunctions;
import com.facebook.presto.operator.scalar.BuiltInScalarFunctionImplementation;
import com.facebook.presto.operator.scalar.CharacterStringCasts;
import com.facebook.presto.operator.scalar.ColorFunctions;
import com.facebook.presto.operator.scalar.CombineHashFunction;
import com.facebook.presto.operator.scalar.DataSizeFunctions;
import com.facebook.presto.operator.scalar.DateTimeFunctions;
import com.facebook.presto.operator.scalar.EmptyMapConstructor;
import com.facebook.presto.operator.scalar.FailureFunction;
import com.facebook.presto.operator.scalar.HmacFunctions;
import com.facebook.presto.operator.scalar.HyperLogLogFunctions;
import com.facebook.presto.operator.scalar.IpPrefixFunctions;
import com.facebook.presto.operator.scalar.JoniRegexpCasts;
import com.facebook.presto.operator.scalar.JoniRegexpFunctions;
import com.facebook.presto.operator.scalar.JoniRegexpReplaceLambdaFunction;
import com.facebook.presto.operator.scalar.JsonFunctions;
import com.facebook.presto.operator.scalar.JsonOperators;
import com.facebook.presto.operator.scalar.KllSketchFunctions;
import com.facebook.presto.operator.scalar.MapCardinalityFunction;
import com.facebook.presto.operator.scalar.MapDistinctFromOperator;
import com.facebook.presto.operator.scalar.MapEntriesFunction;
import com.facebook.presto.operator.scalar.MapEqualOperator;
import com.facebook.presto.operator.scalar.MapFilterFunction;
import com.facebook.presto.operator.scalar.MapFromEntriesFunction;
import com.facebook.presto.operator.scalar.MapIndeterminateOperator;
import com.facebook.presto.operator.scalar.MapKeys;
import com.facebook.presto.operator.scalar.MapNotEqualOperator;
import com.facebook.presto.operator.scalar.MapSubscriptOperator;
import com.facebook.presto.operator.scalar.MapSubsetFunction;
import com.facebook.presto.operator.scalar.MapValues;
import com.facebook.presto.operator.scalar.MathFunctions;
import com.facebook.presto.operator.scalar.MathFunctions.LegacyLogFunction;
import com.facebook.presto.operator.scalar.MultimapFromEntriesFunction;
import com.facebook.presto.operator.scalar.QuantileDigestFunctions;
import com.facebook.presto.operator.scalar.Re2JRegexpFunctions;
import com.facebook.presto.operator.scalar.Re2JRegexpReplaceLambdaFunction;
import com.facebook.presto.operator.scalar.RepeatFunction;
import com.facebook.presto.operator.scalar.SequenceFunction;
import com.facebook.presto.operator.scalar.SessionFunctions;
import com.facebook.presto.operator.scalar.SfmSketchFunctions;
import com.facebook.presto.operator.scalar.SplitToMapFunction;
import com.facebook.presto.operator.scalar.SplitToMultimapFunction;
import com.facebook.presto.operator.scalar.StringFunctions;
import com.facebook.presto.operator.scalar.TDigestFunctions;
import com.facebook.presto.operator.scalar.ThetaSketchFunctions;
import com.facebook.presto.operator.scalar.TryFunction;
import com.facebook.presto.operator.scalar.TypeOfFunction;
import com.facebook.presto.operator.scalar.UrlFunctions;
import com.facebook.presto.operator.scalar.VarbinaryFunctions;
import com.facebook.presto.operator.scalar.WilsonInterval;
import com.facebook.presto.operator.scalar.WordStemFunction;
import com.facebook.presto.operator.scalar.queryplan.JsonPrestoQueryPlanFunctions;
import com.facebook.presto.operator.scalar.sql.ArraySqlFunctions;
import com.facebook.presto.operator.scalar.sql.MapNormalizeFunction;
import com.facebook.presto.operator.scalar.sql.MapSqlFunctions;
import com.facebook.presto.operator.scalar.sql.SimpleSamplingPercent;
import com.facebook.presto.operator.scalar.sql.StringSqlFunctions;
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
import com.facebook.presto.spi.function.AggregationFunctionImplementation;
import com.facebook.presto.spi.function.AlterRoutineCharacteristics;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.FunctionNamespaceManager;
import com.facebook.presto.spi.function.FunctionNamespaceTransactionHandle;
import com.facebook.presto.spi.function.Parameter;
import com.facebook.presto.spi.function.ScalarFunctionImplementation;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlFunction;
import com.facebook.presto.spi.function.SqlFunctionVisibility;
import com.facebook.presto.spi.function.SqlInvokedFunction;
import com.facebook.presto.spi.function.SqlInvokedScalarFunctionImplementation;
import com.facebook.presto.spi.function.table.TableFunctionMetadata;
import com.facebook.presto.spi.tvf.TVFProvider;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.type.BigintOperators;
import com.facebook.presto.type.BooleanOperators;
import com.facebook.presto.type.CharOperators;
import com.facebook.presto.type.CharParametricType;
import com.facebook.presto.type.ColorOperators;
import com.facebook.presto.type.DateOperators;
import com.facebook.presto.type.DateTimeOperators;
import com.facebook.presto.type.DecimalOperators;
import com.facebook.presto.type.DecimalParametricType;
import com.facebook.presto.type.DoubleComparisonOperators;
import com.facebook.presto.type.DoubleOperators;
import com.facebook.presto.type.EnumCasts;
import com.facebook.presto.type.HyperLogLogOperators;
import com.facebook.presto.type.IntegerOperators;
import com.facebook.presto.type.IntervalDayTimeOperators;
import com.facebook.presto.type.IntervalYearMonthOperators;
import com.facebook.presto.type.IpAddressOperators;
import com.facebook.presto.type.IpPrefixOperators;
import com.facebook.presto.type.KllSketchOperators;
import com.facebook.presto.type.LegacyDoubleComparisonOperators;
import com.facebook.presto.type.LegacyRealComparisonOperators;
import com.facebook.presto.type.LikeFunctions;
import com.facebook.presto.type.LongEnumOperators;
import com.facebook.presto.type.MapParametricType;
import com.facebook.presto.type.QuantileDigestOperators;
import com.facebook.presto.type.RealComparisonOperators;
import com.facebook.presto.type.RealOperators;
import com.facebook.presto.type.SfmSketchOperators;
import com.facebook.presto.type.SmallintOperators;
import com.facebook.presto.type.TDigestOperators;
import com.facebook.presto.type.TimeOperators;
import com.facebook.presto.type.TimeWithTimeZoneOperators;
import com.facebook.presto.type.TimestampOperators;
import com.facebook.presto.type.TimestampWithTimeZoneOperators;
import com.facebook.presto.type.TinyintOperators;
import com.facebook.presto.type.UnknownOperators;
import com.facebook.presto.type.UuidOperators;
import com.facebook.presto.type.VarbinaryOperators;
import com.facebook.presto.type.VarcharEnumOperators;
import com.facebook.presto.type.VarcharOperators;
import com.facebook.presto.type.VarcharParametricType;
import com.facebook.presto.type.khyperloglog.KHyperLogLogAggregationFunction;
import com.facebook.presto.type.khyperloglog.KHyperLogLogFunctions;
import com.facebook.presto.type.khyperloglog.KHyperLogLogOperators;
import com.facebook.presto.type.khyperloglog.KHyperLogLogWithLimitAggregationFunction;
import com.facebook.presto.type.khyperloglog.MergeKHyperLogLogAggregationFunction;
import com.facebook.presto.type.khyperloglog.MergeKHyperLogLogWithLimitAggregationFunction;
import com.facebook.presto.type.setdigest.BuildSetDigestAggregation;
import com.facebook.presto.type.setdigest.MergeSetDigestAggregation;
import com.facebook.presto.type.setdigest.SetDigestFunctions;
import com.facebook.presto.type.setdigest.SetDigestOperators;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.util.concurrent.UncheckedExecutionException;
import io.airlift.slice.Slice;

import javax.annotation.concurrent.ThreadSafe;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.facebook.presto.common.function.OperatorType.tryGetOperatorType;
import static com.facebook.presto.common.type.BigintEnumParametricType.BIGINT_ENUM;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.DoubleType.OLD_NAN_DOUBLE;
import static com.facebook.presto.common.type.HyperLogLogType.HYPER_LOG_LOG;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.JsonType.JSON;
import static com.facebook.presto.common.type.KdbTreeType.KDB_TREE;
import static com.facebook.presto.common.type.KllSketchParametricType.KLL_SKETCH;
import static com.facebook.presto.common.type.P4HyperLogLogType.P4_HYPER_LOG_LOG;
import static com.facebook.presto.common.type.QuantileDigestParametricType.QDIGEST;
import static com.facebook.presto.common.type.RealType.OLD_NAN_REAL;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TDigestParametricType.TDIGEST;
import static com.facebook.presto.common.type.TimeType.TIME;
import static com.facebook.presto.common.type.TimeWithTimeZoneType.TIME_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static com.facebook.presto.common.type.UuidType.UUID;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.VarcharEnumParametricType.VARCHAR_ENUM;
import static com.facebook.presto.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static com.facebook.presto.geospatial.type.BingTileType.BING_TILE;
import static com.facebook.presto.geospatial.type.GeometryType.GEOMETRY;
import static com.facebook.presto.operator.aggregation.AlternativeArbitraryAggregationFunction.ALTERNATIVE_ANY_VALUE_AGGREGATION;
import static com.facebook.presto.operator.aggregation.AlternativeArbitraryAggregationFunction.ALTERNATIVE_ARBITRARY_AGGREGATION;
import static com.facebook.presto.operator.aggregation.AlternativeMaxAggregationFunction.ALTERNATIVE_MAX;
import static com.facebook.presto.operator.aggregation.AlternativeMinAggregationFunction.ALTERNATIVE_MIN;
import static com.facebook.presto.operator.aggregation.ArbitraryAggregationFunction.ANY_VALUE_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ArbitraryAggregationFunction.ARBITRARY_AGGREGATION;
import static com.facebook.presto.operator.aggregation.ChecksumAggregationFunction.CHECKSUM_AGGREGATION;
import static com.facebook.presto.operator.aggregation.CountColumn.COUNT_COLUMN;
import static com.facebook.presto.operator.aggregation.DecimalAverageAggregation.DECIMAL_AVERAGE_AGGREGATION;
import static com.facebook.presto.operator.aggregation.DecimalSumAggregation.DECIMAL_SUM_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MapAggregationFunction.MAP_AGG;
import static com.facebook.presto.operator.aggregation.MapUnionAggregation.MAP_UNION;
import static com.facebook.presto.operator.aggregation.MapUnionSumAggregation.MAP_UNION_SUM;
import static com.facebook.presto.operator.aggregation.MaxAggregationFunction.MAX_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MaxNAggregationFunction.MAX_N_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MinAggregationFunction.MIN_AGGREGATION;
import static com.facebook.presto.operator.aggregation.MinNAggregationFunction.MIN_N_AGGREGATION;
import static com.facebook.presto.operator.aggregation.QuantileDigestAggregationFunction.QDIGEST_AGG;
import static com.facebook.presto.operator.aggregation.QuantileDigestAggregationFunction.QDIGEST_AGG_WITH_WEIGHT;
import static com.facebook.presto.operator.aggregation.QuantileDigestAggregationFunction.QDIGEST_AGG_WITH_WEIGHT_AND_ERROR;
import static com.facebook.presto.operator.aggregation.RealAverageAggregation.REAL_AVERAGE_AGGREGATION;
import static com.facebook.presto.operator.aggregation.TDigestAggregationFunction.TDIGEST_AGG;
import static com.facebook.presto.operator.aggregation.TDigestAggregationFunction.TDIGEST_AGG_WITH_WEIGHT;
import static com.facebook.presto.operator.aggregation.TDigestAggregationFunction.TDIGEST_AGG_WITH_WEIGHT_AND_COMPRESSION;
import static com.facebook.presto.operator.aggregation.arrayagg.SetAggregationFunction.SET_AGG;
import static com.facebook.presto.operator.aggregation.arrayagg.SetUnionFunction.SET_UNION;
import static com.facebook.presto.operator.aggregation.minmaxby.AlternativeMaxByAggregationFunction.ALTERNATIVE_MAX_BY;
import static com.facebook.presto.operator.aggregation.minmaxby.AlternativeMinByAggregationFunction.ALTERNATIVE_MIN_BY;
import static com.facebook.presto.operator.aggregation.minmaxby.MaxByAggregationFunction.MAX_BY;
import static com.facebook.presto.operator.aggregation.minmaxby.MaxByNAggregationFunction.MAX_BY_N_AGGREGATION;
import static com.facebook.presto.operator.aggregation.minmaxby.MinByAggregationFunction.MIN_BY;
import static com.facebook.presto.operator.aggregation.minmaxby.MinByNAggregationFunction.MIN_BY_N_AGGREGATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisyAverageGaussianAggregation.NOISY_AVERAGE_GAUSSIAN_AGGREGATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisyAverageGaussianClippingAggregation.NOISY_AVERAGE_GAUSSIAN_CLIPPING_AGGREGATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisyAverageGaussianClippingRandomSeedAggregation.NOISY_AVERAGE_GAUSSIAN_CLIPPING_RANDOM_SEED_AGGREGATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisyAverageGaussianRandomSeedAggregation.NOISY_AVERAGE_GAUSSIAN_RANDOM_SEED_AGGREGATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisyCountGaussianColumnAggregation.NOISY_COUNT_GAUSSIAN_AGGREGATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisyCountGaussianColumnRandomSeedAggregation.NOISY_COUNT_GAUSSIAN_RANDOM_SEED_AGGREGATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisySumGaussianAggregation.NOISY_SUM_GAUSSIAN_AGGREGATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisySumGaussianClippingAggregation.NOISY_SUM_GAUSSIAN_CLIPPING_AGGREGATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisySumGaussianClippingRandomSeedAggregation.NOISY_SUM_GAUSSIAN_CLIPPING_RANDOM_SEED_AGGREGATION;
import static com.facebook.presto.operator.aggregation.noisyaggregation.NoisySumGaussianRandomSeedAggregation.NOISY_SUM_GAUSSIAN_RANDOM_SEED_AGGREGATION;
import static com.facebook.presto.operator.scalar.ArrayConcatFunction.ARRAY_CONCAT_FUNCTION;
import static com.facebook.presto.operator.scalar.ArrayConstructor.ARRAY_CONSTRUCTOR;
import static com.facebook.presto.operator.scalar.ArrayFlattenFunction.ARRAY_FLATTEN_FUNCTION;
import static com.facebook.presto.operator.scalar.ArrayJoin.ARRAY_JOIN;
import static com.facebook.presto.operator.scalar.ArrayJoin.ARRAY_JOIN_WITH_NULL_REPLACEMENT;
import static com.facebook.presto.operator.scalar.ArrayReduceFunction.ARRAY_REDUCE_FUNCTION;
import static com.facebook.presto.operator.scalar.ArraySubscriptOperator.ARRAY_SUBSCRIPT;
import static com.facebook.presto.operator.scalar.ArrayToArrayCast.ARRAY_TO_ARRAY_CAST;
import static com.facebook.presto.operator.scalar.ArrayToElementConcatFunction.ARRAY_TO_ELEMENT_CONCAT_FUNCTION;
import static com.facebook.presto.operator.scalar.ArrayToJsonCast.ARRAY_TO_JSON;
import static com.facebook.presto.operator.scalar.ArrayTransformFunction.ARRAY_TRANSFORM_FUNCTION;
import static com.facebook.presto.operator.scalar.CastFromUnknownOperator.CAST_FROM_UNKNOWN;
import static com.facebook.presto.operator.scalar.ConcatFunction.VARBINARY_CONCAT;
import static com.facebook.presto.operator.scalar.ConcatFunction.VARCHAR_CONCAT;
import static com.facebook.presto.operator.scalar.ElementToArrayConcatFunction.ELEMENT_TO_ARRAY_CONCAT_FUNCTION;
import static com.facebook.presto.operator.scalar.Greatest.GREATEST;
import static com.facebook.presto.operator.scalar.IdentityCast.IDENTITY_CAST;
import static com.facebook.presto.operator.scalar.JsonStringToArrayCast.JSON_STRING_TO_ARRAY;
import static com.facebook.presto.operator.scalar.JsonStringToMapCast.JSON_STRING_TO_MAP;
import static com.facebook.presto.operator.scalar.JsonStringToRowCast.JSON_STRING_TO_ROW;
import static com.facebook.presto.operator.scalar.JsonToArrayCast.JSON_TO_ARRAY;
import static com.facebook.presto.operator.scalar.JsonToMapCast.JSON_TO_MAP;
import static com.facebook.presto.operator.scalar.JsonToRowCast.JSON_TO_ROW;
import static com.facebook.presto.operator.scalar.KDistinct.K_DISTINCT;
import static com.facebook.presto.operator.scalar.Least.LEAST;
import static com.facebook.presto.operator.scalar.MapConcatFunction.MAP_CONCAT_FUNCTION;
import static com.facebook.presto.operator.scalar.MapConstructor.MAP_CONSTRUCTOR;
import static com.facebook.presto.operator.scalar.MapElementAtFunction.MAP_ELEMENT_AT;
import static com.facebook.presto.operator.scalar.MapHashCodeOperator.MAP_HASH_CODE;
import static com.facebook.presto.operator.scalar.MapToJsonCast.MAP_TO_JSON;
import static com.facebook.presto.operator.scalar.MapToMapCast.MAP_TO_MAP_CAST;
import static com.facebook.presto.operator.scalar.MapTransformKeyFunction.MAP_TRANSFORM_KEY_FUNCTION;
import static com.facebook.presto.operator.scalar.MapTransformValueFunction.MAP_TRANSFORM_VALUE_FUNCTION;
import static com.facebook.presto.operator.scalar.MapZipWithFunction.MAP_ZIP_WITH_FUNCTION;
import static com.facebook.presto.operator.scalar.MathFunctions.DECIMAL_MOD_FUNCTION;
import static com.facebook.presto.operator.scalar.Re2JCastToRegexpFunction.castCharToRe2JRegexp;
import static com.facebook.presto.operator.scalar.Re2JCastToRegexpFunction.castVarcharToRe2JRegexp;
import static com.facebook.presto.operator.scalar.RowDistinctFromOperator.ROW_DISTINCT_FROM;
import static com.facebook.presto.operator.scalar.RowEqualOperator.ROW_EQUAL;
import static com.facebook.presto.operator.scalar.RowGreaterThanOperator.ROW_GREATER_THAN;
import static com.facebook.presto.operator.scalar.RowGreaterThanOrEqualOperator.ROW_GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.operator.scalar.RowHashCodeOperator.ROW_HASH_CODE;
import static com.facebook.presto.operator.scalar.RowIndeterminateOperator.ROW_INDETERMINATE;
import static com.facebook.presto.operator.scalar.RowLessThanOperator.ROW_LESS_THAN;
import static com.facebook.presto.operator.scalar.RowLessThanOrEqualOperator.ROW_LESS_THAN_OR_EQUAL;
import static com.facebook.presto.operator.scalar.RowNotEqualOperator.ROW_NOT_EQUAL;
import static com.facebook.presto.operator.scalar.RowToJsonCast.ROW_TO_JSON;
import static com.facebook.presto.operator.scalar.RowToRowCast.ROW_TO_ROW_CAST;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.ArgumentProperty.valueTypeArgumentProperty;
import static com.facebook.presto.operator.scalar.ScalarFunctionImplementationChoice.NullConvention.RETURN_NULL_ON_NULL;
import static com.facebook.presto.operator.scalar.TryCastFunction.TRY_CAST;
import static com.facebook.presto.operator.scalar.ZipFunction.ZIP_FUNCTIONS;
import static com.facebook.presto.operator.scalar.ZipWithFunction.ZIP_WITH_FUNCTION;
import static com.facebook.presto.operator.scalar.distinct.DistinctTypeBetweenOperator.DISTINCT_TYPE_BETWEEN_OPERATOR;
import static com.facebook.presto.operator.scalar.distinct.DistinctTypeDistinctFromOperator.DISTINCT_TYPE_DISTINCT_FROM_OPERATOR;
import static com.facebook.presto.operator.scalar.distinct.DistinctTypeEqualOperator.DISTINCT_TYPE_EQUAL_OPERATOR;
import static com.facebook.presto.operator.scalar.distinct.DistinctTypeGreaterThanOperator.DISTINCT_TYPE_GREATER_THAN_OPERATOR;
import static com.facebook.presto.operator.scalar.distinct.DistinctTypeGreaterThanOrEqualOperator.DISTINCT_TYPE_GREATER_THAN_OR_EQUAL_OPERATOR;
import static com.facebook.presto.operator.scalar.distinct.DistinctTypeHashCodeOperator.DISTINCT_TYPE_HASH_CODE_OPERATOR;
import static com.facebook.presto.operator.scalar.distinct.DistinctTypeIndeterminateOperator.DISTINCT_TYPE_INDETERMINATE_OPERATOR;
import static com.facebook.presto.operator.scalar.distinct.DistinctTypeLessThanOperator.DISTINCT_TYPE_LESS_THAN_OPERATOR;
import static com.facebook.presto.operator.scalar.distinct.DistinctTypeLessThanOrEqualOperator.DISTINCT_TYPE_LESS_THAN_OR_EQUAL_OPERATOR;
import static com.facebook.presto.operator.scalar.distinct.DistinctTypeNotEqualOperator.DISTINCT_TYPE_NOT_EQUAL_OPERATOR;
import static com.facebook.presto.operator.scalar.distinct.DistinctTypeXXHash64Operator.DISTINCT_TYPE_XX_HASH_64_OPERATOR;
import static com.facebook.presto.operator.window.AggregateWindowFunction.supplier;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_MISSING;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.facebook.presto.spi.function.FunctionImplementationType.JAVA;
import static com.facebook.presto.spi.function.FunctionImplementationType.SQL;
import static com.facebook.presto.spi.function.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.function.FunctionKind.SCALAR;
import static com.facebook.presto.spi.function.FunctionKind.WINDOW;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.HIDDEN;
import static com.facebook.presto.sql.planner.LiteralEncoder.MAGIC_LITERAL_FUNCTION_PREFIX;
import static com.facebook.presto.type.ArrayParametricType.ARRAY;
import static com.facebook.presto.type.CodePointsType.CODE_POINTS;
import static com.facebook.presto.type.ColorType.COLOR;
import static com.facebook.presto.type.DecimalCasts.BIGINT_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.BOOLEAN_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_BIGINT_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_BOOLEAN_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_DOUBLE_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_INTEGER_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_JSON_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_REAL_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_SMALLINT_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_TINYINT_CAST;
import static com.facebook.presto.type.DecimalCasts.DECIMAL_TO_VARCHAR_CAST;
import static com.facebook.presto.type.DecimalCasts.DOUBLE_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.INTEGER_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.JSON_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.REAL_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.SMALLINT_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.TINYINT_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalCasts.VARCHAR_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_BETWEEN_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_DISTINCT_FROM_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_EQUAL_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_GREATER_THAN_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_LESS_THAN_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR;
import static com.facebook.presto.type.DecimalInequalityOperators.DECIMAL_NOT_EQUAL_OPERATOR;
import static com.facebook.presto.type.DecimalOperators.DECIMAL_ADD_OPERATOR;
import static com.facebook.presto.type.DecimalOperators.DECIMAL_DIVIDE_OPERATOR;
import static com.facebook.presto.type.DecimalOperators.DECIMAL_MODULUS_OPERATOR;
import static com.facebook.presto.type.DecimalOperators.DECIMAL_MULTIPLY_OPERATOR;
import static com.facebook.presto.type.DecimalOperators.DECIMAL_SUBTRACT_OPERATOR;
import static com.facebook.presto.type.DecimalSaturatedFloorCasts.BIGINT_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static com.facebook.presto.type.DecimalSaturatedFloorCasts.DECIMAL_TO_BIGINT_SATURATED_FLOOR_CAST;
import static com.facebook.presto.type.DecimalSaturatedFloorCasts.DECIMAL_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static com.facebook.presto.type.DecimalSaturatedFloorCasts.DECIMAL_TO_INTEGER_SATURATED_FLOOR_CAST;
import static com.facebook.presto.type.DecimalSaturatedFloorCasts.DECIMAL_TO_SMALLINT_SATURATED_FLOOR_CAST;
import static com.facebook.presto.type.DecimalSaturatedFloorCasts.DECIMAL_TO_TINYINT_SATURATED_FLOOR_CAST;
import static com.facebook.presto.type.DecimalSaturatedFloorCasts.INTEGER_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static com.facebook.presto.type.DecimalSaturatedFloorCasts.SMALLINT_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static com.facebook.presto.type.DecimalSaturatedFloorCasts.TINYINT_TO_DECIMAL_SATURATED_FLOOR_CAST;
import static com.facebook.presto.type.DecimalToDecimalCasts.DECIMAL_TO_DECIMAL_CAST;
import static com.facebook.presto.type.DistinctTypeCasts.DISTINCT_TYPE_FROM_CAST;
import static com.facebook.presto.type.DistinctTypeCasts.DISTINCT_TYPE_TO_CAST;
import static com.facebook.presto.type.FunctionParametricType.FUNCTION;
import static com.facebook.presto.type.IntervalDayTimeType.INTERVAL_DAY_TIME;
import static com.facebook.presto.type.IntervalYearMonthType.INTERVAL_YEAR_MONTH;
import static com.facebook.presto.type.IpAddressType.IPADDRESS;
import static com.facebook.presto.type.IpPrefixType.IPPREFIX;
import static com.facebook.presto.type.JoniRegexpType.JONI_REGEXP;
import static com.facebook.presto.type.JsonPathType.JSON_PATH;
import static com.facebook.presto.type.LikePatternType.LIKE_PATTERN;
import static com.facebook.presto.type.MapParametricType.MAP;
import static com.facebook.presto.type.Re2JRegexpType.RE2J_REGEXP;
import static com.facebook.presto.type.RowParametricType.ROW;
import static com.facebook.presto.type.SfmSketchType.SFM_SKETCH;
import static com.facebook.presto.type.khyperloglog.KHyperLogLogType.K_HYPER_LOG_LOG;
import static com.facebook.presto.type.setdigest.SetDigestType.SET_DIGEST;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static com.google.common.base.Throwables.throwIfUnchecked;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.HOURS;

@ThreadSafe
public class BuiltInTypeAndFunctionNamespaceManager
        implements FunctionNamespaceManager<SqlFunction>, TVFProvider, TypeManager
{
    public static final CatalogSchemaName JAVA_BUILTIN_NAMESPACE = new CatalogSchemaName("presto", "default");
    public static final String ID = "builtin";

    private final FunctionAndTypeManager functionAndTypeManager;

    private final ConcurrentMap<TypeSignature, Type> types = new ConcurrentHashMap<>();
    private final ConcurrentMap<String, ParametricType> parametricTypes = new ConcurrentHashMap<>();

    private final LoadingCache<Signature, SpecializedFunctionKey> specializedFunctionKeyCache;
    private final LoadingCache<SpecializedFunctionKey, ScalarFunctionImplementation> specializedScalarCache;
    private final LoadingCache<SpecializedFunctionKey, AggregationFunctionImplementation> specializedAggregationCache;
    private final LoadingCache<SpecializedFunctionKey, WindowFunctionSupplier> specializedWindowCache;
    private final LoadingCache<ExactTypeSignature, Type> parametricTypeCache;
    private final MagicLiteralFunction magicLiteralFunction;

    private volatile FunctionMap functions = new FunctionMap();

    public BuiltInTypeAndFunctionNamespaceManager(
            BlockEncodingSerde blockEncodingSerde,
            FunctionsConfig functionsConfig,
            Set<Type> types,
            FunctionAndTypeManager functionAndTypeManager)
    {
        this.functionAndTypeManager = requireNonNull(functionAndTypeManager, "functionAndTypeManager is null");
        this.magicLiteralFunction = new MagicLiteralFunction(blockEncodingSerde);

        specializedFunctionKeyCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(this::doGetSpecializedFunctionKey));

        // TODO the function map should be updated, so that this cast can be removed

        // We have observed repeated compilation of MethodHandle that leads to full GCs.
        // We notice that flushing the following caches mitigate the problem.
        // We suspect that it is a JVM bug that is related to stale/corrupted profiling data associated
        // with generated classes and/or dynamically-created MethodHandles.
        // This might also mitigate problems like deoptimization storm or unintended interpreted execution.

        specializedScalarCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(key -> {
                    checkArgument(
                            key.getFunction() instanceof SqlScalarFunction || key.getFunction() instanceof SqlInvokedFunction,
                            "Unsupported scalar function class: %s",
                            key.getFunction().getClass());
                    return key.getFunction() instanceof SqlScalarFunction
                            ? ((SqlScalarFunction) key.getFunction()).specialize(key.getBoundVariables(), key.getArity(), functionAndTypeManager)
                            : new SqlInvokedScalarFunctionImplementation(((SqlInvokedFunction) key.getFunction()).getBody());
                }));

        specializedAggregationCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(key -> ((SqlAggregationFunction) key.getFunction())
                        .specialize(key.getBoundVariables(), key.getArity(), functionAndTypeManager)));

        specializedWindowCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(key ->
                {
                    if (key.getFunction() instanceof SqlAggregationFunction) {
                        return supplier(key.getFunction().getSignature(), specializedAggregationCache.getUnchecked(key));
                    }
                    return ((SqlWindowFunction) key.getFunction())
                            .specialize(key.getBoundVariables(), key.getArity(), functionAndTypeManager);
                }));

        parametricTypeCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(1, HOURS)
                .build(CacheLoader.from(this::instantiateParametricType));

        registerBuiltInFunctions(getBuiltInFunctions(functionsConfig));
        registerBuiltInTypes(functionsConfig);

        for (Type type : requireNonNull(types, "types is null")) {
            addType(type);
        }
    }

    private void registerBuiltInTypes(FunctionsConfig functionsConfig)
    {
        // always add the built-in types; Presto will not function without these
        addType(UNKNOWN);
        addType(BOOLEAN);
        addType(BIGINT);
        addType(INTEGER);
        addType(SMALLINT);
        addType(TINYINT);
        if (!functionsConfig.getUseNewNanDefinition()) {
            addType(OLD_NAN_DOUBLE);
            addType(OLD_NAN_REAL);
        }
        else {
            addType(DOUBLE);
            addType(REAL);
        }
        addType(VARBINARY);
        addType(DATE);
        addType(TIME);
        addType(TIME_WITH_TIME_ZONE);
        addType(TIMESTAMP);
        addType(TIMESTAMP_WITH_TIME_ZONE);
        addType(INTERVAL_YEAR_MONTH);
        addType(INTERVAL_DAY_TIME);
        addType(HYPER_LOG_LOG);
        addType(SET_DIGEST);
        addType(K_HYPER_LOG_LOG);
        addType(P4_HYPER_LOG_LOG);
        addType(SFM_SKETCH);
        addType(JONI_REGEXP);
        addType(RE2J_REGEXP);
        addType(LIKE_PATTERN);
        addType(JSON_PATH);
        addType(COLOR);
        addType(JSON);
        addType(CODE_POINTS);
        addType(IPADDRESS);
        addType(IPPREFIX);
        addType(UUID);
        addType(GEOMETRY);
        addType(BING_TILE);
        addType(KDB_TREE);
        addType(SPHERICAL_GEOGRAPHY);
        addParametricType(VarcharParametricType.VARCHAR);
        addParametricType(CharParametricType.CHAR);
        addParametricType(DecimalParametricType.DECIMAL);
        addParametricType(ROW);
        addParametricType(ARRAY);
        addParametricType(MAP);
        addParametricType(FUNCTION);
        addParametricType(QDIGEST);
        addParametricType(TDIGEST);
        addParametricType(KLL_SKETCH);
        addParametricType(BIGINT_ENUM);
        addParametricType(VARCHAR_ENUM);
    }

    private List<? extends SqlFunction> getBuiltInFunctions(FunctionsConfig functionsConfig)
    {
        FunctionListBuilder builder = new FunctionListBuilder()
                .window(RowNumberFunction.class)
                .window(RankFunction.class)
                .window(DenseRankFunction.class)
                .window(PercentRankFunction.class)
                .window(CumulativeDistributionFunction.class)
                .window(NTileFunction.class)
                .window(FirstValueFunction.class)
                .window(LastValueFunction.class)
                .window(NthValueFunction.class)
                .window(LagFunction.class)
                .window(LeadFunction.class)
                .aggregate(ApproximateCountDistinctAggregation.class)
                .aggregate(DefaultApproximateCountDistinctAggregation.class)
                .aggregate(NoisyApproximateSetSfmAggregation.class)
                .aggregate(NoisyApproximateSetSfmAggregationDefaultBucketsPrecision.class)
                .aggregate(NoisyApproximateSetSfmAggregationDefaultPrecision.class)
                .aggregate(NoisyApproximateSetSfmFromIndexAndZerosAggregation.class)
                .aggregate(NoisyApproximateSetSfmFromIndexAndZerosAggregationDefaultPrecision.class)
                .aggregate(NoisyApproximateDistinctCountSfmAggregation.class)
                .aggregate(NoisyApproximateDistinctCountSfmAggregationDefaultBucketsPrecision.class)
                .aggregate(NoisyApproximateDistinctCountSfmAggregationDefaultPrecision.class)
                .aggregate(SumDataSizeForStats.class)
                .aggregate(MaxDataSizeForStats.class)
                .aggregate(ConvexHullAggregation.class)
                .aggregate(GeometryUnionAgg.class)
                .aggregate(SpatialPartitioningAggregateFunction.class)
                .aggregate(SpatialPartitioningInternalAggregateFunction.class)
                .aggregates(CountAggregation.class)
                .aggregates(VarianceAggregation.class)
                .aggregates(CentralMomentsAggregation.class)
                .aggregates(CountIfAggregation.class)
                .aggregates(BooleanAndAggregation.class)
                .aggregates(BooleanOrAggregation.class)
                .aggregates(DoubleSumAggregation.class)
                .aggregates(RealSumAggregation.class)
                .aggregates(LongSumAggregation.class)
                .aggregates(IntervalDayToSecondSumAggregation.class)
                .aggregates(IntervalYearToMonthSumAggregation.class)
                .aggregates(AverageAggregations.class)
                .function(NOISY_SUM_GAUSSIAN_AGGREGATION)
                .function(NOISY_SUM_GAUSSIAN_RANDOM_SEED_AGGREGATION)
                .function(NOISY_SUM_GAUSSIAN_CLIPPING_AGGREGATION)
                .function(NOISY_SUM_GAUSSIAN_CLIPPING_RANDOM_SEED_AGGREGATION)
                .function(NOISY_AVERAGE_GAUSSIAN_AGGREGATION)
                .function(NOISY_AVERAGE_GAUSSIAN_CLIPPING_AGGREGATION)
                .function(NOISY_AVERAGE_GAUSSIAN_CLIPPING_RANDOM_SEED_AGGREGATION)
                .function(NOISY_AVERAGE_GAUSSIAN_RANDOM_SEED_AGGREGATION)
                .function(REAL_AVERAGE_AGGREGATION)
                .aggregates(IntervalDayToSecondAverageAggregation.class)
                .aggregates(IntervalYearToMonthAverageAggregation.class)
                .aggregates(DifferentialEntropyAggregation.class)
                .aggregates(EntropyAggregation.class)
                .aggregates(GeometricMeanAggregations.class)
                .aggregates(RealGeometricMeanAggregations.class)
                .aggregates(MergeHyperLogLogAggregation.class)
                .aggregates(SfmSketchMergeAggregation.class)
                .aggregates(ApproximateSetAggregation.class)
                .functions(QDIGEST_AGG, QDIGEST_AGG_WITH_WEIGHT, QDIGEST_AGG_WITH_WEIGHT_AND_ERROR)
                .function(MergeQuantileDigestFunction.MERGE)
                .aggregates(DoubleHistogramAggregation.class)
                .aggregates(RealHistogramAggregation.class)
                .aggregates(DoubleCovarianceAggregation.class)
                .aggregates(RealCovarianceAggregation.class)
                .aggregates(DoubleRegressionAggregation.class)
                .aggregates(RealRegressionAggregation.class)
                .aggregates(DoubleCorrelationAggregation.class)
                .aggregates(RealCorrelationAggregation.class)
                .aggregates(BitwiseOrAggregation.class)
                .aggregates(BitwiseAndAggregation.class)
                .aggregates(BitwiseXorAggregation.class)
                .aggregates(ClassificationMissRateAggregation.class)
                .aggregates(ClassificationFallOutAggregation.class)
                .aggregates(ClassificationPrecisionAggregation.class)
                .aggregates(ClassificationRecallAggregation.class)
                .aggregates(ClassificationThresholdsAggregation.class)
                .scalar(ArrayCumSum.class)
                .scalar(RepeatFunction.class)
                .scalars(SequenceFunction.class)
                .scalars(SessionFunctions.class)
                .scalars(StringFunctions.class)
                .scalars(WordStemFunction.class)
                .scalar(SplitToMapFunction.ResolveDuplicateKeys.class)
                .scalar(SplitToMapFunction.FailOnDuplicateKeys.class)
                .scalar(SplitToMultimapFunction.class)
                .scalars(VarbinaryFunctions.class)
                .scalars(UrlFunctions.class)
                .scalars(MathFunctions.class)
                .scalar(MathFunctions.Abs.class)
                .scalar(MathFunctions.Sign.class)
                .scalar(MathFunctions.Round.class)
                .scalar(MathFunctions.RoundN.class)
                .scalar(MathFunctions.Truncate.class)
                .scalar(MathFunctions.TruncateN.class)
                .scalar(MathFunctions.Ceiling.class)
                .scalar(MathFunctions.Floor.class)
                .scalars(BitwiseFunctions.class)
                .scalars(DateTimeFunctions.class)
                .scalars(JsonFunctions.class)
                .scalars(JsonPrestoQueryPlanFunctions.class)
                .scalars(ColorFunctions.class)
                .scalars(ColorOperators.class)
                .scalars(GeoFunctions.class)
                .scalars(BingTileFunctions.class)
                .scalars(BingTileOperators.class)
                .scalar(BingTileFunctions.BingTileCoordinatesFunction.class)
                .scalars(SphericalGeoFunctions.class)
                .scalars(KdbTreeCasts.class)
                .scalar(ColorOperators.ColorDistinctFromOperator.class)
                .scalars(HyperLogLogFunctions.class)
                .scalars(SfmSketchFunctions.class)
                .scalars(QuantileDigestFunctions.class)
                .scalars(UnknownOperators.class)
                .scalar(UnknownOperators.UnknownDistinctFromOperator.class)
                .scalars(BooleanOperators.class)
                .scalar(BooleanOperators.BooleanDistinctFromOperator.class)
                .scalars(BigintOperators.class)
                .scalar(BigintOperators.BigintDistinctFromOperator.class)
                .scalars(IntegerOperators.class)
                .scalar(IntegerOperators.IntegerDistinctFromOperator.class)
                .scalars(SmallintOperators.class)
                .scalar(SmallintOperators.SmallintDistinctFromOperator.class)
                .scalars(TinyintOperators.class)
                .scalar(TinyintOperators.TinyintDistinctFromOperator.class)
                .scalars(DoubleOperators.class)
                .scalars(RealOperators.class);

        if (functionsConfig.getUseNewNanDefinition()) {
            builder.scalars(DoubleComparisonOperators.class)
                    .scalar(DoubleComparisonOperators.DoubleDistinctFromOperator.class)
                    .scalars(RealComparisonOperators.class)
                    .scalar(RealComparisonOperators.RealDistinctFromOperator.class);
        }
        else {
            builder.scalars(LegacyDoubleComparisonOperators.class)
                    .scalar(LegacyDoubleComparisonOperators.DoubleDistinctFromOperator.class)
                    .scalars(LegacyRealComparisonOperators.class)
                    .scalar(LegacyRealComparisonOperators.RealDistinctFromOperator.class);
        }
        builder.scalars(VarcharOperators.class)
                .scalar(VarcharOperators.VarcharDistinctFromOperator.class)
                .scalars(VarbinaryOperators.class)
                .scalar(VarbinaryOperators.VarbinaryDistinctFromOperator.class)
                .scalars(DateOperators.class)
                .scalar(DateOperators.DateDistinctFromOperator.class)
                .scalars(TimeOperators.class)
                .scalar(TimeOperators.TimeDistinctFromOperator.class)
                .scalars(TimestampOperators.class)
                .scalar(TimestampOperators.TimestampDistinctFromOperator.class)
                .scalars(IntervalDayTimeOperators.class)
                .scalar(IntervalDayTimeOperators.IntervalDayTimeDistinctFromOperator.class)
                .scalars(IntervalYearMonthOperators.class)
                .scalar(IntervalYearMonthOperators.IntervalYearMonthDistinctFromOperator.class)
                .scalars(TimeWithTimeZoneOperators.class)
                .scalar(TimeWithTimeZoneOperators.TimeWithTimeZoneDistinctFromOperator.class)
                .scalars(TimestampWithTimeZoneOperators.class)
                .scalar(TimestampWithTimeZoneOperators.TimestampWithTimeZoneDistinctFromOperator.class)
                .scalars(DateTimeOperators.class)
                .scalars(HyperLogLogOperators.class)
                .scalars(SfmSketchOperators.class)
                .scalars(QuantileDigestOperators.class)
                .scalars(IpAddressOperators.class)
                .scalar(IpAddressOperators.IpAddressDistinctFromOperator.class)
                .scalars(IpPrefixFunctions.class)
                .scalars(IpPrefixOperators.class)
                .scalar(IpPrefixOperators.IpPrefixDistinctFromOperator.class)
                .scalars(UuidOperators.class)
                .scalar(UuidOperators.UuidDistinctFromOperator.class)
                .scalars(LikeFunctions.class)
                .scalars(ArrayFunctions.class)
                .scalars(HmacFunctions.class)
                .scalars(DataSizeFunctions.class)
                .scalar(ArrayCardinalityFunction.class)
                .scalar(ArrayContains.class)
                .scalar(ArrayFilterFunction.class)
                .scalar(ArrayPositionFunction.class)
                .scalar(ArrayPositionWithIndexFunction.class)
                .scalar(ArraySumBigIntFunction.class)
                .scalar(ArraySumDoubleFunction.class)
                .scalars(CombineHashFunction.class)
                .scalars(JsonOperators.class)
                .scalar(JsonOperators.JsonDistinctFromOperator.class)
                .scalars(FailureFunction.class)
                .scalars(JoniRegexpCasts.class)
                .scalars(CharacterStringCasts.class)
                .scalars(CharOperators.class)
                .scalar(CharOperators.CharDistinctFromOperator.class)
                .scalar(DecimalOperators.Negation.class)
                .scalar(DecimalOperators.HashCode.class)
                .scalar(DecimalOperators.Indeterminate.class)
                .scalar(DecimalOperators.XxHash64Operator.class)
                .functions(IDENTITY_CAST, CAST_FROM_UNKNOWN)
                .scalar(ArrayLessThanOperator.class)
                .scalar(ArrayLessThanOrEqualOperator.class)
                .scalar(ArrayRemoveFunction.class)
                .scalar(ArrayGreaterThanOperator.class)
                .scalar(ArrayGreaterThanOrEqualOperator.class)
                .scalar(ArrayElementAtFunction.class)
                .scalar(ArraySortFunction.class)
                .scalar(MapSubsetFunction.class)
                .scalar(ArraySortComparatorFunction.class)
                .scalar(ArrayShuffleFunction.class)
                .scalar(ArrayReverseFunction.class)
                .scalar(ArrayMinFunction.class)
                .scalar(ArrayMaxFunction.class)
                .scalar(ArrayDistinctFunction.class)
                .scalar(ArrayNotEqualOperator.class)
                .scalar(ArrayEqualOperator.class)
                .scalar(ArrayHashCodeOperator.class)
                .scalars(ArrayIntersectFunction.class)
                .scalar(ArraysOverlapFunction.class)
                .scalar(ArrayDistinctFromOperator.class)
                .scalar(ArrayUnionFunction.class)
                .scalar(ArrayExceptFunction.class)
                .scalar(ArraySliceFunction.class)
                .scalar(ArrayTrimFunction.class)
                .scalar(ArrayIndeterminateOperator.class)
                .scalar(ArrayCombinationsFunction.class)
                .scalar(ArrayNgramsFunction.class)
                .scalar(ArrayAllMatchFunction.class)
                .scalar(ArrayAnyMatchFunction.class)
                .scalar(ArrayFindFirstFunction.class)
                .scalar(ArrayFindFirstWithOffsetFunction.class)
                .scalar(ArrayFindFirstIndexFunction.class)
                .scalar(ArrayFindFirstIndexWithOffsetFunction.class)
                .scalar(ArrayNoneMatchFunction.class)
                .scalar(ArrayNormalizeFunction.class)
                .scalar(MapDistinctFromOperator.class)
                .scalar(MapEqualOperator.class)
                .scalar(MapEntriesFunction.class)
                .scalar(MapFromEntriesFunction.class)
                .scalar(MultimapFromEntriesFunction.class)
                .scalar(MapNotEqualOperator.class)
                .scalar(MapKeys.class)
                .scalar(MapValues.class)
                .scalar(MapCardinalityFunction.class)
                .scalar(EmptyMapConstructor.class)
                .scalar(MapIndeterminateOperator.class)
                .scalar(TypeOfFunction.class)
                .scalar(TryFunction.class)
                .functions(ZIP_WITH_FUNCTION, MAP_ZIP_WITH_FUNCTION)
                .functions(ZIP_FUNCTIONS)
                .functions(ARRAY_JOIN, ARRAY_JOIN_WITH_NULL_REPLACEMENT)
                .functions(ARRAY_TO_ARRAY_CAST)
                .functions(ARRAY_TO_ELEMENT_CONCAT_FUNCTION, ELEMENT_TO_ARRAY_CONCAT_FUNCTION)
                .function(MAP_HASH_CODE)
                .function(MAP_ELEMENT_AT)
                .function(MAP_CONCAT_FUNCTION)
                .function(MAP_TO_MAP_CAST)
                .function(ARRAY_FLATTEN_FUNCTION)
                .function(ARRAY_CONCAT_FUNCTION)
                .functions(ARRAY_CONSTRUCTOR, ARRAY_SUBSCRIPT, ARRAY_TO_JSON, JSON_TO_ARRAY, JSON_STRING_TO_ARRAY)
                .functions(SET_AGG, SET_UNION)
                .function(new ArrayAggregationFunction(functionsConfig.isLegacyArrayAgg(), functionsConfig.getArrayAggGroupImplementation()))
                .functions(new MapSubscriptOperator(functionsConfig.isLegacyMapSubscript()))
                .functions(MAP_CONSTRUCTOR, MAP_TO_JSON, JSON_TO_MAP, JSON_STRING_TO_MAP)
                .functions(MAP_AGG, MAP_UNION, MAP_UNION_SUM)
                .function(new ReduceAggregationFunction(functionsConfig.isReduceAggForComplexTypesEnabled()))
                .functions(DECIMAL_TO_VARCHAR_CAST, DECIMAL_TO_INTEGER_CAST, DECIMAL_TO_BIGINT_CAST, DECIMAL_TO_DOUBLE_CAST, DECIMAL_TO_REAL_CAST, DECIMAL_TO_BOOLEAN_CAST, DECIMAL_TO_TINYINT_CAST, DECIMAL_TO_SMALLINT_CAST)
                .functions(VARCHAR_TO_DECIMAL_CAST, INTEGER_TO_DECIMAL_CAST, BIGINT_TO_DECIMAL_CAST, DOUBLE_TO_DECIMAL_CAST, REAL_TO_DECIMAL_CAST, BOOLEAN_TO_DECIMAL_CAST, TINYINT_TO_DECIMAL_CAST, SMALLINT_TO_DECIMAL_CAST)
                .functions(JSON_TO_DECIMAL_CAST, DECIMAL_TO_JSON_CAST)
                .functions(DECIMAL_ADD_OPERATOR, DECIMAL_SUBTRACT_OPERATOR, DECIMAL_MULTIPLY_OPERATOR, DECIMAL_DIVIDE_OPERATOR, DECIMAL_MODULUS_OPERATOR)
                .functions(DECIMAL_EQUAL_OPERATOR, DECIMAL_NOT_EQUAL_OPERATOR)
                .functions(DECIMAL_LESS_THAN_OPERATOR, DECIMAL_LESS_THAN_OR_EQUAL_OPERATOR)
                .functions(DECIMAL_GREATER_THAN_OPERATOR, DECIMAL_GREATER_THAN_OR_EQUAL_OPERATOR)
                .function(DECIMAL_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .functions(DECIMAL_TO_BIGINT_SATURATED_FLOOR_CAST, BIGINT_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .functions(DECIMAL_TO_INTEGER_SATURATED_FLOOR_CAST, INTEGER_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .functions(DECIMAL_TO_SMALLINT_SATURATED_FLOOR_CAST, SMALLINT_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .functions(DECIMAL_TO_TINYINT_SATURATED_FLOOR_CAST, TINYINT_TO_DECIMAL_SATURATED_FLOOR_CAST)
                .function(DECIMAL_BETWEEN_OPERATOR)
                .function(DECIMAL_DISTINCT_FROM_OPERATOR)
                .function(new Histogram(functionsConfig.getHistogramGroupImplementation()))
                .function(CHECKSUM_AGGREGATION)
                .function(IDENTITY_CAST)
                .function(ARBITRARY_AGGREGATION)
                .function(ANY_VALUE_AGGREGATION)
                .functions(GREATEST, LEAST)
                .functions(MAX_BY, MIN_BY, MAX_BY_N_AGGREGATION, MIN_BY_N_AGGREGATION)
                .functions(MAX_AGGREGATION, MIN_AGGREGATION, MAX_N_AGGREGATION, MIN_N_AGGREGATION)
                .function(COUNT_COLUMN)
                .function(NOISY_COUNT_GAUSSIAN_AGGREGATION)
                .function(NOISY_COUNT_GAUSSIAN_RANDOM_SEED_AGGREGATION)
                .aggregates(NoisyCountIfGaussianAggregation.class)
                .functions(ROW_HASH_CODE, ROW_TO_JSON, JSON_TO_ROW, JSON_STRING_TO_ROW, ROW_DISTINCT_FROM, ROW_EQUAL, ROW_GREATER_THAN, ROW_GREATER_THAN_OR_EQUAL, ROW_LESS_THAN, ROW_LESS_THAN_OR_EQUAL, ROW_NOT_EQUAL, ROW_TO_ROW_CAST, ROW_INDETERMINATE)
                .functions(VARCHAR_CONCAT, VARBINARY_CONCAT)
                .function(DECIMAL_TO_DECIMAL_CAST)
                .function(castVarcharToRe2JRegexp(functionsConfig.getRe2JDfaStatesLimit(), functionsConfig.getRe2JDfaRetries()))
                .function(castCharToRe2JRegexp(functionsConfig.getRe2JDfaStatesLimit(), functionsConfig.getRe2JDfaRetries()))
                .function(DECIMAL_AVERAGE_AGGREGATION)
                .function(DECIMAL_SUM_AGGREGATION)
                .function(DECIMAL_MOD_FUNCTION)
                .functions(ARRAY_TRANSFORM_FUNCTION, ARRAY_REDUCE_FUNCTION)
                .functions(MAP_TRANSFORM_KEY_FUNCTION, MAP_TRANSFORM_VALUE_FUNCTION)
                .function(TRY_CAST)
                .aggregate(ApproximateMostFrequent.class)
                .function(K_DISTINCT)
                .aggregate(MergeSetDigestAggregation.class)
                .aggregate(BuildSetDigestAggregation.class)
                .scalars(SetDigestFunctions.class)
                .scalars(SetDigestOperators.class)
                .scalars(KHyperLogLogFunctions.class)
                .scalars(KHyperLogLogOperators.class)
                .scalars(WilsonInterval.class)
                .scalars(TDigestOperators.class)
                .scalars(TDigestFunctions.class)
                .functions(TDIGEST_AGG, TDIGEST_AGG_WITH_WEIGHT, TDIGEST_AGG_WITH_WEIGHT_AND_COMPRESSION)
                .aggregate(ThetaSketchAggregationFunction.class)
                .scalars(ThetaSketchFunctions.class)
                .function(MergeTDigestFunction.MERGE)
                .sqlInvokedScalar(MapNormalizeFunction.class)
                .sqlInvokedScalars(ArraySqlFunctions.class)
                .sqlInvokedScalars(ArrayIntersectFunction.class)
                .sqlInvokedScalars(MapSqlFunctions.class)
                .sqlInvokedScalars(SimpleSamplingPercent.class)
                .sqlInvokedScalars(StringSqlFunctions.class)
                .scalar(DynamicFilterPlaceholderFunction.class)
                .scalars(EnumCasts.class)
                .scalars(LongEnumOperators.class)
                .scalars(VarcharEnumOperators.class)
                .functions(DISTINCT_TYPE_FROM_CAST, DISTINCT_TYPE_TO_CAST)
                .functions(DISTINCT_TYPE_EQUAL_OPERATOR, DISTINCT_TYPE_NOT_EQUAL_OPERATOR)
                .functions(DISTINCT_TYPE_LESS_THAN_OPERATOR, DISTINCT_TYPE_LESS_THAN_OR_EQUAL_OPERATOR)
                .functions(DISTINCT_TYPE_GREATER_THAN_OPERATOR, DISTINCT_TYPE_GREATER_THAN_OR_EQUAL_OPERATOR)
                .functions(DISTINCT_TYPE_BETWEEN_OPERATOR)
                .function(DISTINCT_TYPE_DISTINCT_FROM_OPERATOR)
                .functions(DISTINCT_TYPE_HASH_CODE_OPERATOR, DISTINCT_TYPE_XX_HASH_64_OPERATOR)
                .function(DISTINCT_TYPE_INDETERMINATE_OPERATOR)
                .codegenScalars(MapFilterFunction.class)
                .aggregate(ReservoirSampleFunction.class)
                .aggregate(KllSketchAggregationFunction.class)
                .aggregate(KllSketchWithKAggregationFunction.class)
                .scalars(KllSketchFunctions.class)
                .scalars(KllSketchOperators.class);

        switch (functionsConfig.getRegexLibrary()) {
            case JONI:
                builder.scalars(JoniRegexpFunctions.class);
                builder.scalar(JoniRegexpReplaceLambdaFunction.class);
                break;
            case RE2J:
                builder.scalars(Re2JRegexpFunctions.class);
                builder.scalar(Re2JRegexpReplaceLambdaFunction.class);
                break;
        }

        if (functionsConfig.isLegacyLogFunction()) {
            builder.scalar(LegacyLogFunction.class);
        }

        // Replace some aggregations for Velox to override intermediate aggregation type.
        if (functionsConfig.isUseAlternativeFunctionSignatures()) {
            builder.override(ARBITRARY_AGGREGATION, ALTERNATIVE_ARBITRARY_AGGREGATION);
            builder.override(ANY_VALUE_AGGREGATION, ALTERNATIVE_ANY_VALUE_AGGREGATION);
            builder.override(MAX_AGGREGATION, ALTERNATIVE_MAX);
            builder.override(MIN_AGGREGATION, ALTERNATIVE_MIN);
            builder.override(MAX_BY, ALTERNATIVE_MAX_BY);
            builder.override(MIN_BY, ALTERNATIVE_MIN_BY);
            builder.functions(AlternativeApproxPercentile.getFunctions());
            builder.function(new AlternativeMultimapAggregationFunction(functionsConfig.getMultimapAggGroupImplementation()));
        }
        else {
            builder.aggregates(ApproximateLongPercentileAggregations.class);
            builder.aggregates(ApproximateLongPercentileArrayAggregations.class);
            builder.aggregates(ApproximateDoublePercentileAggregations.class);
            builder.aggregates(ApproximateDoublePercentileArrayAggregations.class);
            builder.aggregates(ApproximateRealPercentileAggregations.class);
            builder.aggregates(ApproximateRealPercentileArrayAggregations.class);
            builder.function(new MultimapAggregationFunction(functionsConfig.getMultimapAggGroupImplementation()));
        }

        if (functionsConfig.getLimitNumberOfGroupsForKHyperLogLogAggregations()) {
            builder.function(new MergeKHyperLogLogWithLimitAggregationFunction(functionsConfig.getKHyperLogLogAggregationGroupNumberLimit()));
            builder.function(new KHyperLogLogWithLimitAggregationFunction(functionsConfig.getKHyperLogLogAggregationGroupNumberLimit()));
        }
        else {
            builder.aggregates(MergeKHyperLogLogAggregationFunction.class);
            builder.aggregates(KHyperLogLogAggregationFunction.class);
        }

        return builder.getFunctions();
    }

    public synchronized void registerBuiltInFunctions(List<? extends SqlFunction> functions)
    {
        for (SqlFunction function : functions) {
            for (SqlFunction existingFunction : this.functions.list()) {
                checkArgument(!function.getSignature().equals(existingFunction.getSignature()), "Function already registered: %s", function.getSignature());
            }
        }
        this.functions = new FunctionMap(this.functions, functions);
    }

    @Override
    public void setBlockEncodingSerde(BlockEncodingSerde blockEncodingSerde)
    {
        // Do not need to do anything here since BlockEncodingSerde is passed in constructor
    }

    @Override
    public void createFunction(SqlInvokedFunction function, boolean replace)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Cannot create function in built-in function namespace: %s", function.getSignature().getName()));
    }

    @Override
    public void alterFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, AlterRoutineCharacteristics alterRoutineCharacteristics)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Cannot alter function in built-in function namespace: %s", functionName));
    }

    @Override
    public void dropFunction(QualifiedObjectName functionName, Optional<List<TypeSignature>> parameterTypes, boolean exists)
    {
        throw new PrestoException(GENERIC_USER_ERROR, format("Cannot drop function in built-in function namespace: %s", functionName));
    }

    public String getName()
    {
        return ID;
    }

    @Override
    public FunctionNamespaceTransactionHandle beginTransaction()
    {
        return new EmptyTransactionHandle();
    }

    @Override
    public void commit(FunctionNamespaceTransactionHandle transactionHandle)
    {
    }

    @Override
    public void abort(FunctionNamespaceTransactionHandle transactionHandle)
    {
    }

    /**
     * likePattern / escape is not used for optimization, returning all functions.
     */
    @Override
    public Collection<SqlFunction> listFunctions(Optional<String> likePattern, Optional<String> escape)
    {
        return functions.list();
    }

    @Override
    public Collection<SqlFunction> getFunctions(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, QualifiedObjectName functionName)
    {
        return functions.get(functionName);
    }

    @Override
    public FunctionHandle getFunctionHandle(Optional<? extends FunctionNamespaceTransactionHandle> transactionHandle, Signature signature)
    {
        return new BuiltInFunctionHandle(signature);
    }

    @Override
    public FunctionMetadata getFunctionMetadata(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof BuiltInFunctionHandle, "Expect BuiltInFunctionHandle");
        Signature signature = ((BuiltInFunctionHandle) functionHandle).getSignature();
        SpecializedFunctionKey functionKey;
        try {
            functionKey = specializedFunctionKeyCache.getUnchecked(signature);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
        SqlFunction function = functionKey.getFunction();
        Optional<OperatorType> operatorType = tryGetOperatorType(signature.getName());
        if (operatorType.isPresent()) {
            return new FunctionMetadata(
                    operatorType.get(),
                    signature.getArgumentTypes(),
                    signature.getReturnType(),
                    signature.getKind(),
                    JAVA,
                    function.isDeterministic(),
                    function.isCalledOnNullInput(),
                    function.getComplexTypeFunctionDescriptor());
        }
        else if (function instanceof SqlInvokedFunction) {
            SqlInvokedFunction sqlFunction = (SqlInvokedFunction) function;
            List<String> argumentNames = sqlFunction.getParameters().stream().map(Parameter::getName).collect(toImmutableList());
            return new FunctionMetadata(
                    signature.getName(),
                    signature.getArgumentTypes(),
                    argumentNames,
                    signature.getReturnType(),
                    signature.getKind(),
                    sqlFunction.getRoutineCharacteristics().getLanguage(),
                    SQL,
                    function.isDeterministic(),
                    function.isCalledOnNullInput(),
                    sqlFunction.getVersion(),
                    sqlFunction.getComplexTypeFunctionDescriptor());
        }
        else {
            return new FunctionMetadata(
                    signature.getName(),
                    signature.getArgumentTypes(),
                    signature.getReturnType(),
                    signature.getKind(),
                    JAVA,
                    function.isDeterministic(),
                    function.isCalledOnNullInput(),
                    function.getComplexTypeFunctionDescriptor());
        }
    }

    @Override
    public final CompletableFuture<SqlFunctionResult> executeFunction(String source, FunctionHandle functionHandle, Page input, List<Integer> channels, TypeManager typeManager)
    {
        throw new IllegalStateException("Builtin function execution should be handled by the engine.");
    }

    @Override
    public void addUserDefinedType(UserDefinedType userDefinedType)
    {
        throw new UnsupportedOperationException("User defined type is not supported");
    }

    @Override
    public Optional<UserDefinedType> getUserDefinedType(QualifiedObjectName typeName)
    {
        throw new UnsupportedOperationException("User defined type is not supported");
    }

    public WindowFunctionSupplier getWindowFunctionImplementation(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof BuiltInFunctionHandle, "Expect BuiltInFunctionHandle");
        Signature signature = ((BuiltInFunctionHandle) functionHandle).getSignature();
        checkArgument(signature.getKind() == WINDOW || signature.getKind() == AGGREGATE, "%s is not a window function", signature);
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);

        try {
            return specializedWindowCache.getUnchecked(getSpecializedFunctionKey(signature));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    public AggregationFunctionImplementation getAggregateFunctionImplementation(FunctionHandle functionHandle, TypeManager typeManager)
    {
        checkArgument(functionHandle instanceof BuiltInFunctionHandle, "Expect BuiltInFunctionHandle");
        Signature signature = ((BuiltInFunctionHandle) functionHandle).getSignature();
        checkArgument(signature.getKind() == AGGREGATE, "%s is not an aggregate function", signature);
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);

        try {
            return specializedAggregationCache.getUnchecked(getSpecializedFunctionKey(signature));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    @Override
    public ScalarFunctionImplementation getScalarFunctionImplementation(FunctionHandle functionHandle)
    {
        checkArgument(functionHandle instanceof BuiltInFunctionHandle, "Expect BuiltInFunctionHandle");
        return getScalarFunctionImplementation(((BuiltInFunctionHandle) functionHandle).getSignature());
    }

    public ScalarFunctionImplementation getScalarFunctionImplementation(Signature signature)
    {
        checkArgument(signature.getKind() == SCALAR, "%s is not a scalar function", signature);
        checkArgument(signature.getTypeVariableConstraints().isEmpty(), "%s has unbound type parameters", signature);

        try {
            return specializedScalarCache.getUnchecked(getSpecializedFunctionKey(signature));
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    @Override
    public Type getType(TypeSignature typeSignature)
    {
        Type type = types.get(typeSignature);
        if (type != null) {
            return type;
        }
        try {
            return parametricTypeCache.getUnchecked(new ExactTypeSignature(typeSignature));
        }
        catch (UncheckedExecutionException e) {
            throwIfUnchecked(e.getCause());
            throw new RuntimeException(e.getCause());
        }
    }

    @Override
    public Type getParameterizedType(String baseTypeName, List<TypeSignatureParameter> typeParameters)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean canCoerce(Type actualType, Type expectedType)
    {
        throw new UnsupportedOperationException();
    }

    public List<Type> getTypes()
    {
        return ImmutableList.copyOf(types.values());
    }

    public void addType(Type type)
    {
        requireNonNull(type, "type is null");
        Type existingType = types.putIfAbsent(type.getTypeSignature(), type);
        checkState(existingType == null || existingType.equals(type), "Type %s is already registered", type);
    }

    public void addParametricType(ParametricType parametricType)
    {
        String name = parametricType.getName().toLowerCase(Locale.ENGLISH);
        checkArgument(!parametricTypes.containsKey(name), "Parametric type already registered: %s", name);
        parametricTypes.putIfAbsent(name, parametricType);
    }

    @Override
    public Collection<ParametricType> getParametricTypes()
    {
        return parametricTypes.values();
    }

    private Type instantiateParametricType(ExactTypeSignature exactTypeSignature)
    {
        return instantiateParametricType(exactTypeSignature.getTypeSignature(), functionAndTypeManager, parametricTypes);
    }

    public Type instantiateParametricType(
            TypeSignature signature,
            FunctionAndTypeManager functionAndTypeManager,
            Map<String, ParametricType> parametricTypes)
    {
        List<TypeParameter> parameters = new ArrayList<>();

        for (TypeSignatureParameter parameter : signature.getParameters()) {
            TypeParameter typeParameter = TypeParameter.of(parameter, functionAndTypeManager);
            parameters.add(typeParameter);
        }

        ParametricType parametricType = parametricTypes.get(signature.getBase().toLowerCase(Locale.ENGLISH));
        if (parametricType == null) {
            throw new UnknownTypeException(signature);
        }

        if (parametricType instanceof MapParametricType) {
            return ((MapParametricType) parametricType).createType(functionAndTypeManager, parameters);
        }

        Type instantiatedType = parametricType.createType(parameters);

        // TODO: reimplement this check? Currently "varchar(Integer.MAX_VALUE)" fails with "varchar"
        //checkState(instantiatedType.equalsSignature(signature), "Instantiated parametric type name (%s) does not match expected name (%s)", instantiatedType, signature);
        return instantiatedType;
    }

    private SpecializedFunctionKey getSpecializedFunctionKey(Signature signature)
    {
        try {
            return specializedFunctionKeyCache.getUnchecked(signature);
        }
        catch (UncheckedExecutionException e) {
            throwIfInstanceOf(e.getCause(), PrestoException.class);
            throw e;
        }
    }

    private SpecializedFunctionKey doGetSpecializedFunctionKey(Signature signature)
    {
        return functionAndTypeManager.getSpecializedFunctionKey(signature);
    }

    public SpecializedFunctionKey doGetSpecializedFunctionKeyForMagicLiteralFunctions(Signature signature, FunctionAndTypeManager functionAndTypeManager)
    {
        // TODO: this is a hack and should be removed
        if (signature.getNameSuffix().startsWith(MAGIC_LITERAL_FUNCTION_PREFIX)) {
            List<TypeSignature> parameterTypes = signature.getArgumentTypes();
            // extract type from function name
            String typeName = signature.getNameSuffix().substring(MAGIC_LITERAL_FUNCTION_PREFIX.length());

            // lookup the type
            Type type = functionAndTypeManager.getType(parseTypeSignature(typeName));

            // verify we have one parameter of the proper type
            checkArgument(parameterTypes.size() == 1, "Expected one argument to literal function, but got %s", parameterTypes);
            Type parameterType = functionAndTypeManager.getType(parameterTypes.get(0));
            requireNonNull(parameterType, format("Type %s not found", parameterTypes.get(0)));

            return new SpecializedFunctionKey(
                    magicLiteralFunction,
                    BoundVariables.builder()
                            .setTypeVariable("T", parameterType)
                            .setTypeVariable("R", type)
                            .build(),
                    1);
        }
        throw new PrestoException(FUNCTION_IMPLEMENTATION_MISSING, format("%s not found", signature));
    }

    @Override
    public TableFunctionMetadata resolveTableFunction(String functionName)
    {
        return null;
    }

    private static class EmptyTransactionHandle
            implements FunctionNamespaceTransactionHandle
    {
    }

    private static class FunctionMap
    {
        private final Multimap<QualifiedObjectName, SqlFunction> functions;

        public FunctionMap()
        {
            functions = ImmutableListMultimap.of();
        }

        public FunctionMap(FunctionMap map, Iterable<? extends SqlFunction> functions)
        {
            this.functions = ImmutableListMultimap.<QualifiedObjectName, SqlFunction>builder()
                    .putAll(map.functions)
                    .putAll(Multimaps.index(functions, function -> function.getSignature().getName()))
                    .build();

            // Make sure all functions with the same name are aggregations or none of them are
            for (Map.Entry<QualifiedObjectName, Collection<SqlFunction>> entry : this.functions.asMap().entrySet()) {
                Collection<SqlFunction> values = entry.getValue();
                long aggregations = values.stream()
                        .map(function -> function.getSignature().getKind())
                        .filter(kind -> kind == AGGREGATE)
                        .count();
                checkState(aggregations == 0 || aggregations == values.size(), "'%s' is both an aggregation and a scalar function", entry.getKey());
            }
        }

        public List<SqlFunction> list()
        {
            return ImmutableList.copyOf(functions.values());
        }

        public Collection<SqlFunction> get(QualifiedObjectName name)
        {
            return functions.get(name);
        }
    }

    /**
     * TypeSignature but has overridden equals(). Here, we compare exact signature of any underlying distinct
     * types. Some distinct types may have extra information on their lazily loaded parents, and same parent
     * information is compared in equals(). This is needed to cache types in parametricTypesCache.
     */
    private static class ExactTypeSignature
    {
        private final TypeSignature typeSignature;

        public ExactTypeSignature(TypeSignature typeSignature)
        {
            this.typeSignature = typeSignature;
        }

        public TypeSignature getTypeSignature()
        {
            return typeSignature;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(typeSignature);
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            ExactTypeSignature other = (ExactTypeSignature) o;
            return equals(typeSignature, other.typeSignature);
        }

        private static boolean equals(TypeSignature left, TypeSignature right)
        {
            if (!left.equals(right)) {
                return false;
            }

            if (left.isDistinctType() && right.isDistinctType()) {
                return equals(left.getDistinctTypeInfo(), right.getDistinctTypeInfo());
            }
            int index = 0;
            for (TypeSignatureParameter leftParameter : left.getParameters()) {
                TypeSignatureParameter rightParameter = right.getParameters().get(index++);
                if (!leftParameter.getKind().equals(rightParameter.getKind())) {
                    return false;
                }

                switch (leftParameter.getKind()) {
                    case TYPE:
                        if (!equals(leftParameter.getTypeSignature(), rightParameter.getTypeSignature())) {
                            return false;
                        }
                        break;
                    case NAMED_TYPE:
                        if (!equals(leftParameter.getNamedTypeSignature().getTypeSignature(), rightParameter.getNamedTypeSignature().getTypeSignature())) {
                            return false;
                        }
                        break;
                    case DISTINCT_TYPE:
                        if (!equals(leftParameter.getDistinctTypeInfo(), rightParameter.getDistinctTypeInfo())) {
                            return false;
                        }
                        break;
                }
            }
            return true;
        }

        private static boolean equals(DistinctTypeInfo left, DistinctTypeInfo right)
        {
            return Objects.equals(left.getName(), right.getName()) &&
                    Objects.equals(left.getBaseType(), right.getBaseType()) &&
                    Objects.equals(left.isOrderable(), right.isOrderable()) &&
                    Objects.equals(left.getTopMostAncestor(), right.getTopMostAncestor()) &&
                    Objects.equals(left.getOtherAncestors(), right.getOtherAncestors());
        }
    }

    private static class MagicLiteralFunction
            extends SqlScalarFunction
    {
        private final BlockEncodingSerde blockEncodingSerde;

        MagicLiteralFunction(BlockEncodingSerde blockEncodingSerde)
        {
            super(new Signature(QualifiedObjectName.valueOf(JAVA_BUILTIN_NAMESPACE, MAGIC_LITERAL_FUNCTION_PREFIX), SCALAR, TypeSignature.parseTypeSignature("R"), TypeSignature.parseTypeSignature("T")));
            this.blockEncodingSerde = requireNonNull(blockEncodingSerde, "blockEncodingSerde is null");
        }

        @Override
        public final SqlFunctionVisibility getVisibility()
        {
            return HIDDEN;
        }

        @Override
        public boolean isDeterministic()
        {
            return true;
        }

        @Override
        public String getDescription()
        {
            return "magic literal";
        }

        @Override
        public BuiltInScalarFunctionImplementation specialize(BoundVariables boundVariables, int arity, FunctionAndTypeManager functionAndTypeManager)
        {
            Type parameterType = boundVariables.getTypeVariable("T");
            Type type = boundVariables.getTypeVariable("R");

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

            return new BuiltInScalarFunctionImplementation(
                    false,
                    ImmutableList.of(valueTypeArgumentProperty(RETURN_NULL_ON_NULL)),
                    methodHandle);
        }
    }
}
