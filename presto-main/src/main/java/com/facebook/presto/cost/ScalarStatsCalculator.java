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

import com.facebook.presto.FullConnectorSession;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.BuiltInFunctionHandle;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.ScalarFunctionStatsUtils;
import com.facebook.presto.spi.function.ScalarPropagateSourceStats;
import com.facebook.presto.spi.function.ScalarStatsHeader;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.StatsPropagationBehavior;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.NoOpVariableResolver;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.type.TypeUtils;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;

import static com.facebook.presto.common.function.OperatorType.DIVIDE;
import static com.facebook.presto.common.function.OperatorType.MODULUS;
import static com.facebook.presto.cost.StatsUtil.toStatsRepresentation;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.ROW_COUNT;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.UNKNOWN;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.USE_SOURCE_STATS;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.USE_TYPE_WIDTH_VARCHAR;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.getSourceLocation;
import static com.facebook.presto.sql.planner.LiteralInterpreter.evaluate;
import static com.facebook.presto.sql.relational.Expressions.isNull;
import static com.facebook.presto.util.MoreMath.maxExcludingNaNs;
import static com.facebook.presto.util.MoreMath.minExcludingNaNs;
import static com.facebook.presto.util.MoreMath.sumExcludingNaNs;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.NaN;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.abs;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class ScalarStatsCalculator
{
    private final Metadata metadata;

    @Inject
    public ScalarStatsCalculator(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata can not be null");
    }

    @Deprecated
    public VariableStatsEstimate calculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics, Session session, TypeProvider types)
    {
        return new ExpressionStatsVisitor(inputStatistics, session, types).process(scalarExpression);
    }

    public VariableStatsEstimate calculate(RowExpression scalarExpression, PlanNodeStatsEstimate inputStatistics, Session session)
    {
        return scalarExpression.accept(new RowExpressionStatsVisitor(inputStatistics, session.toConnectorSession()), null);
    }

    public VariableStatsEstimate calculate(RowExpression scalarExpression, PlanNodeStatsEstimate inputStatistics, ConnectorSession session)
    {
        return scalarExpression.accept(new RowExpressionStatsVisitor(inputStatistics, session), null);
    }

    private class RowExpressionStatsVisitor
            implements RowExpressionVisitor<VariableStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final ConnectorSession session;
        private final FunctionResolution resolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());

        public RowExpressionStatsVisitor(PlanNodeStatsEstimate input, ConnectorSession session)
        {
            this.input = requireNonNull(input, "input is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public VariableStatsEstimate visitCall(CallExpression call, Void context)
        {
            if (resolution.isNegateFunction(call.getFunctionHandle())) {
                return computeNegationStatistics(call, context);
            }

            FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(call.getFunctionHandle());
            if (functionMetadata.getOperatorType().map(OperatorType::isArithmeticOperator).orElse(false)) {
                return computeArithmeticBinaryStatistics(call, context);
            }

            RowExpression value = new RowExpressionOptimizer(metadata).optimize(call, OPTIMIZED, session);

            if (isNull(value)) {
                return nullStatsEstimate();
            }

            if (value instanceof ConstantExpression) {
                return value.accept(this, context);
            }

            // value is not a constant, but we can still propagate estimation through cast
            if (resolution.isCastFunction(call.getFunctionHandle())) {
                return computeCastStatistics(call, context);
            }

            return computeStatsViaAnnotations(call, context, functionMetadata);
        }

        private VariableStatsEstimate computeStatsViaAnnotations(CallExpression call, Void context, FunctionMetadata functionMetadata)
        {
            // casting session to FullConnectorSession is not ideal.
            boolean isStatsPropagationEnabled =
                    SystemSessionProperties.shouldEnableScalarFunctionStatsPropagation(((FullConnectorSession) session).getSession());

            if (isStatsPropagationEnabled) {
                if (functionMetadata.getOperatorType().map(OperatorType::isHashOperator).orElse(false)) {
                    return computeHashCodeOperatorStatistics(call, context);
                }

                if (functionMetadata.getOperatorType().map(OperatorType::isComparisonOperator).orElse(false)) {
                    return computeComparisonOperatorStatistics(call, context);
                }

                if (call.getDisplayName().equals("concat")) {
                    return computeConcatStatistics(call, context);
                }
                if (functionMetadata.hasStatsHeader() && call.getFunctionHandle() instanceof BuiltInFunctionHandle) {
                    Signature signature = ((BuiltInFunctionHandle) call.getFunctionHandle()).getSignature().canonicalization();
                    Optional<ScalarStatsHeader> statsHeader = functionMetadata.getStatsHeader(signature);
                    if (statsHeader.isPresent()) {
                        return computeCallStatistics(call, context, statsHeader.get());
                    }
                }
                else {
                    System.out.println("Stats not found for func: " + functionMetadata.getName() + " " + call);
                }
            }
            return VariableStatsEstimate.unknown();
        }

        private VariableStatsEstimate getSourceStats(CallExpression call, Void context, int argumentIndex)
        {
            Preconditions.checkArgument(argumentIndex < call.getArguments().size(),
                    "function argument index: " + argumentIndex + " >= " + call.getArguments().size() + " for " + call);
            return call.getArguments().get(argumentIndex).accept(this, context);
        }

        @Override
        public VariableStatsEstimate visitInputReference(InputReferenceExpression reference, Void context)
        {
            throw new UnsupportedOperationException("symbol stats estimation should not reach channel mapping");
        }

        @Override
        public VariableStatsEstimate visitConstant(ConstantExpression literal, Void context)
        {
            if (literal.getValue() == null) {
                return nullStatsEstimate();
            }

            OptionalDouble doubleValue = toStatsRepresentation(metadata.getFunctionAndTypeManager(), session, literal.getType(), literal.getValue());
            VariableStatsEstimate.Builder estimate = VariableStatsEstimate.builder()
                    .setNullsFraction(0)
                    .setDistinctValuesCount(1);

            if (doubleValue.isPresent()) {
                estimate.setLowValue(doubleValue.getAsDouble());
                estimate.setHighValue(doubleValue.getAsDouble());
            }
            return estimate.build();
        }

        @Override
        public VariableStatsEstimate visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return VariableStatsEstimate.unknown();
        }

        @Override
        public VariableStatsEstimate visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return input.getVariableStatistics(reference);
        }

        @Override
        public VariableStatsEstimate visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            if (specialForm.getForm().equals(COALESCE)) {
                VariableStatsEstimate result = null;
                for (RowExpression operand : specialForm.getArguments()) {
                    VariableStatsEstimate operandEstimates = operand.accept(this, context);
                    if (result != null) {
                        result = estimateCoalesce(input, result, operandEstimates);
                    }
                    else {
                        result = operandEstimates;
                    }
                }
                return requireNonNull(result, "result is null");
            }
            return VariableStatsEstimate.unknown();
        }

        private double processMinValue(CallExpression call, Void context, ScalarStatsHeader statsHeader)
        {
            if (isFinite(statsHeader.getMin())) {
                return statsHeader.getMin();
            }
            double minValue = NaN;

            for (Map.Entry<Integer, ScalarPropagateSourceStats> paramIndexVsStatsMap : statsHeader.getArgumentStats().entrySet()) {
                ScalarPropagateSourceStats scalarPropagateSourceStats = paramIndexVsStatsMap.getValue();
                if (scalarPropagateSourceStats.propagateAllStats() || scalarPropagateSourceStats.minValue() == USE_SOURCE_STATS) {
                    return getSourceStats(call, context, paramIndexVsStatsMap.getKey()).getLowValue();
                }
                StatsPropagationBehavior operation = scalarPropagateSourceStats.minValue();
                if (!operation.isSingleArgumentStats()) {
                    for (int i = 0; i < call.getArguments().size(); i++) {
                        VariableStatsEstimate sourceStats = getSourceStats(call, context, i);
                        if (!sourceStats.isUnknown() && isFinite(sourceStats.getLowValue())) {
                            switch (operation) {
                                case USE_MIN_ARGUMENT:
                                    minValue = minExcludingNaNs(minValue, sourceStats.getLowValue());
                                    break;
                                case SUM_ARGUMENTS:
                                    minValue = sumExcludingNaNs(minValue, sourceStats.getLowValue());
                                    break;
                            }
                        }
                    }
                }
            }
            return minValue;
        }

        private double typeWidthVarchar(CallExpression call, int argumentIndex)
        {
            TypeSignature typeSignature = call.getArguments().get(argumentIndex).getType().getTypeSignature();
            if (typeSignature.getTypeSignatureBase().hasStandardType() && typeSignature.getTypeSignatureBase().getStandardTypeBase().equals(StandardTypes.VARCHAR)) {
                if (typeSignature.getParameters().size() == 1) { // Varchar type should have 1 parameter i.e. size.;
                    Long longLiteral = typeSignature.getParameters().get(0).getLongLiteral();
                    if (longLiteral > 0 && longLiteral != VarcharType.UNBOUNDED_LENGTH) {
                        return longLiteral;
                    }
                }
            }
            return NaN;
        }

        private double processMaxValue(CallExpression call, Void context, ScalarStatsHeader statsHeader)
        {
            if (isFinite(statsHeader.getMax())) {
                return statsHeader.getMax();
            }
            double maxValue = NaN;

            for (Map.Entry<Integer, ScalarPropagateSourceStats> paramIndexToSourceStatsMap : statsHeader.getArgumentStats().entrySet()) {
                ScalarPropagateSourceStats scalarPropagateSourceStats = paramIndexToSourceStatsMap.getValue();
                if (scalarPropagateSourceStats.maxValue() == ROW_COUNT) {
                    return input.getOutputRowCount();
                }

                Integer argumentIndex = paramIndexToSourceStatsMap.getKey();

                if (scalarPropagateSourceStats.maxValue() == USE_TYPE_WIDTH_VARCHAR) {
                    return typeWidthVarchar(call, argumentIndex);
                }
                else if (scalarPropagateSourceStats.propagateAllStats() || scalarPropagateSourceStats.maxValue() == USE_SOURCE_STATS) {
                    return getSourceStats(call, context, argumentIndex).getHighValue();
                }
                StatsPropagationBehavior operation = scalarPropagateSourceStats.maxValue();
                if (!operation.isSingleArgumentStats()) {
                    for (int i = 0; i < call.getArguments().size(); i++) {
                        VariableStatsEstimate sourceStats = getSourceStats(call, context, i);
                        if (!sourceStats.isUnknown() && isFinite(sourceStats.getHighValue())) {
                            switch (operation) {
                                case USE_MIN_ARGUMENT:
                                    maxValue = maxExcludingNaNs(maxValue, sourceStats.getHighValue());
                                    break;
                                case SUM_ARGUMENTS:
                                    maxValue = sumExcludingNaNs(maxValue, sourceStats.getHighValue());
                                    break;
                                case MAX_TYPE_WIDTH_VARCHAR:
                                    maxValue = maxExcludingNaNs(maxValue, typeWidthVarchar(call, i));
                            }
                        }
                    }
                }
            }
            return maxValue;
        }

        private StatisticRange processDistinctValuesCountAndRange(
                CallExpression call,
                Void context,
                double nullFraction,
                ScalarStatsHeader statsHeader)
        {
            nullFraction = maxExcludingNaNs(0.0, nullFraction); // if null fraction is NaN, safe to assume 0.0 for ndv calculations.
            checkArgument(nullFraction <= 1.0, "null fraction cannot be greater than 1 " + nullFraction);
            double min = processMinValue(call, context, statsHeader);
            double max = processMaxValue(call, context, statsHeader);
            StatisticRange statisticRange = StatisticRange.empty();
            double distinctValuesCount = NaN;
            if (isFinite(statsHeader.getDistinctValuesCount())) {
                distinctValuesCount = statsHeader.getDistinctValuesCount();
                if (distinctValuesCount == ScalarFunctionStatsUtils.ROW_COUNT_TIMES_INV_NULL_FRACTION) {
                    distinctValuesCount = input.getOutputRowCount() * (1 - nullFraction);
                }
                return new StatisticRange(min, max, distinctValuesCount);
            }
            for (Map.Entry<Integer, ScalarPropagateSourceStats> paramIndexVsStatsMap : statsHeader.getArgumentStats().entrySet()) {
                ScalarPropagateSourceStats scalarPropagateSourceStats = paramIndexVsStatsMap.getValue();
                StatsPropagationBehavior operation = scalarPropagateSourceStats.distinctValuesCount();
                if (!operation.isSingleArgumentStats()) {
                    for (int i = 0; i < call.getArguments().size(); i++) {
                        VariableStatsEstimate sourceStats = getSourceStats(call, context, i);
                        if (!sourceStats.isUnknown() && isFinite(sourceStats.getDistinctValuesCount())) {
                            switch (operation) {
                                case MAX_TYPE_WIDTH_VARCHAR:
                                    distinctValuesCount = maxExcludingNaNs(distinctValuesCount, typeWidthVarchar(call, i));
                                    break;
                                case USE_MAX_ARGUMENT:
                                    statisticRange = statisticRange.addAndMaxDistinctValues(sourceStats.statisticRange());
                                    break;
                                case SUM_ARGUMENTS:
                                    distinctValuesCount = sumExcludingNaNs(distinctValuesCount, sourceStats.getDistinctValuesCount());
                                    break;
                            }
                        }
                    }
                }
                else {
                    VariableStatsEstimate sourceStats = getSourceStats(call, context, paramIndexVsStatsMap.getKey());
                    switch (operation) {
                        case UNKNOWN:
                            if (scalarPropagateSourceStats.propagateAllStats()) {
                                statisticRange = sourceStats.statisticRange();
                            }
                            break;
                        case USE_SOURCE_STATS:
                            statisticRange = sourceStats.statisticRange();
                            break;
                        case ROW_COUNT:
                            statisticRange = new StatisticRange(min, max, input.getOutputRowCount());
                            break;
                        case NON_NULL_ROW_COUNT:
                            statisticRange = new StatisticRange(min, max, input.getOutputRowCount() * (1 - nullFraction));
                            break;
                        case USE_TYPE_WIDTH_VARCHAR:
                            statisticRange = new StatisticRange(min, max, typeWidthVarchar(call, paramIndexVsStatsMap.getKey()));
                            break;
                    }
                }
            }

            if (statisticRange.isEmpty() && isFinite(distinctValuesCount)) {
                if (isFinite(min) && isFinite(max)) {
                    statisticRange = new StatisticRange(min, max, distinctValuesCount);
                }
                else {
                    statisticRange = new StatisticRange(NaN, NaN, distinctValuesCount);
                }
            }
            return statisticRange;
        }

        private double processNullFraction(CallExpression call, Void context, ScalarStatsHeader statsHeader)
        {
            double nullFraction = NaN;
            if (isFinite(statsHeader.getNullFraction())) {
                return statsHeader.getNullFraction();
            }
            else {
                for (Map.Entry<Integer, ScalarPropagateSourceStats> paramIndexVsStatsMap : statsHeader.getArgumentStats().entrySet()) {
                    ScalarPropagateSourceStats scalarPropagateSourceStats = paramIndexVsStatsMap.getValue();
                    StatsPropagationBehavior operation = scalarPropagateSourceStats.nullFraction();
                    if (!operation.isSingleArgumentStats()) {
                        for (int i = 0; i < call.getArguments().size(); i++) {
                            VariableStatsEstimate sourceStats = getSourceStats(call, context, i);
                            if (!sourceStats.isUnknown() && isFinite(sourceStats.getNullsFraction())) {
                                switch (operation) {
                                    case USE_MAX_ARGUMENT:
                                        nullFraction = maxExcludingNaNs(nullFraction, sourceStats.getNullsFraction());
                                        break;
                                    case SUM_ARGUMENTS:
                                        nullFraction = sumExcludingNaNs(nullFraction, sourceStats.getNullsFraction());
                                        break;
                                }
                            }
                        }
                    }
                    else {
                        VariableStatsEstimate sourceStats = getSourceStats(call, context, paramIndexVsStatsMap.getKey());
                        if (operation == USE_SOURCE_STATS || (operation == UNKNOWN && scalarPropagateSourceStats.propagateAllStats())) {
                            nullFraction = sourceStats.getNullsFraction();
                        }
                    }
                }
            }
            return nullFraction;
        }

        private double getReturnTypeWidth(CallExpression call)
        {
            if (call.getType() instanceof FixedWidthType) {
                return ((FixedWidthType) call.getType()).getFixedSize();
            }
            if (call.getType() instanceof VarcharType) {
                VarcharType returnType = (VarcharType) call.getType();
                if (!returnType.isUnbounded()) {
                    return returnType.getLengthSafe();
                }
                if (call.getDisplayName().equals("concat")) {
                    // since return type is a varchar and length is unknown, if function is concat.
                    // try to get an upper bound by summing length of args.
                    double sum = 0;
                    for (RowExpression r : call.getArguments()) {
                        if (r instanceof CallExpression) { // argument is another function call
                            sum += getReturnTypeWidth((CallExpression) r);
                        }
                        if (r.getType() instanceof VarcharType) {
                            VarcharType argType = (VarcharType) r.getType();
                            if (!argType.isUnbounded()) {
                                sum += argType.getLengthSafe();
                            }
                        }
                    }
                    if (sum > 0) {
                        return sum;
                    }
                }
            }
            return NaN;
        }

        private double processAvgRowSize(CallExpression call, Void context, ScalarStatsHeader statsHeader)
        {
            double avgRowSize = NaN;
            if (isFinite(statsHeader.getAvgRowSize())) {
                return statsHeader.getAvgRowSize();
            }

            for (Map.Entry<Integer, ScalarPropagateSourceStats> paramIndexVsStatsMap : statsHeader.getArgumentStats().entrySet()) {
                ScalarPropagateSourceStats scalarPropagateSourceStats = paramIndexVsStatsMap.getValue();
                StatsPropagationBehavior operation = scalarPropagateSourceStats.avgRowSize();
                if (!operation.isSingleArgumentStats()) {
                    for (int i = 0; i < call.getArguments().size(); i++) {
                        VariableStatsEstimate sourceStats = getSourceStats(call, context, i);
                        if (!sourceStats.isUnknown() && isFinite(sourceStats.getAverageRowSize())) {
                            double s1 = sourceStats.getAverageRowSize();
                            switch (operation) {
                                case USE_MAX_ARGUMENT:
                                    avgRowSize = maxExcludingNaNs(avgRowSize, s1);
                                    break;
                                case SUM_ARGUMENTS:
                                    if (isNaN(avgRowSize)) {
                                        avgRowSize = 0;
                                    }
                                    avgRowSize = avgRowSize + s1;
                                    break;
                            }
                        }
                    }
                }
                else {
                    VariableStatsEstimate sourceStats = getSourceStats(call, context, paramIndexVsStatsMap.getKey());
                    if (operation == USE_SOURCE_STATS || (operation == UNKNOWN && scalarPropagateSourceStats.propagateAllStats())) {
                        avgRowSize = sourceStats.getAverageRowSize();
                    }
                }
            }

            return minExcludingNaNs(avgRowSize, getReturnTypeWidth(call)); // avg row size cannot be greater than functions return type size.
        }

        private VariableStatsEstimate computeCallStatistics(CallExpression call, Void context, ScalarStatsHeader statsHeader)
        {
            requireNonNull(call, "call is null");

            // TODO: handle histograms.
            double nullFraction = processNullFraction(call, context, statsHeader);
            VariableStatsEstimate sourceStatsSum = VariableStatsEstimate.builder()
                    .setStatisticsRange(processDistinctValuesCountAndRange(call, context, nullFraction, statsHeader))
                    .setAverageRowSize(processAvgRowSize(call, context, statsHeader))
                    .setNullsFraction(nullFraction)
                    .build();
            System.out.println("call=" + call + " StatsEstimate=" + sourceStatsSum);
            return sourceStatsSum;
        }

        private VariableStatsEstimate computeConcatStatistics(CallExpression call, Void context)
        {  // Concat function is specially handled since it is a generated function for all arity.
            double nullFraction = NaN;
            double ndv = NaN;
            double avgRowSize = 0.0;
            for (RowExpression r : call.getArguments()) {
                VariableStatsEstimate sourceStats = r.accept(this, context);
                if (isFinite(sourceStats.getNullsFraction())) {
                    // concat function returns null if any of the argument is null. So null fraction should add up.
                    nullFraction = maxExcludingNaNs(nullFraction, 0.0);
                    nullFraction += sourceStats.getNullsFraction();
                }
                if (isFinite(sourceStats.getDistinctValuesCount())) {
                    ndv = maxExcludingNaNs(ndv, sourceStats.getDistinctValuesCount());
                }
                if (isFinite(sourceStats.getAverageRowSize())) {
                    avgRowSize += sourceStats.getAverageRowSize();
                }
            }
            if (avgRowSize == 0.0) {
                avgRowSize = NaN;
            }
            return VariableStatsEstimate.builder()
                    .setNullsFraction(nullFraction)
                    .setDistinctValuesCount(minExcludingNaNs(ndv, input.getOutputRowCount()))
                    .setAverageRowSize(minExcludingNaNs(getReturnTypeWidth(call), avgRowSize))
                    .build();
        }

        private VariableStatsEstimate computeCastStatistics(CallExpression call, Void context)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate sourceStats = getSourceStats(call, context, 0);

            // todo - make this general postprocessing rule.
            double distinctValuesCount = sourceStats.getDistinctValuesCount();
            double lowValue = sourceStats.getLowValue();
            double highValue = sourceStats.getHighValue();

            if (TypeUtils.isIntegralType(call.getType().getTypeSignature(), metadata.getFunctionAndTypeManager())) {
                // todo handle low/high value changes if range gets narrower due to cast (e.g. BIGINT -> SMALLINT)
                if (isFinite(lowValue)) {
                    lowValue = Math.round(lowValue);
                }
                if (isFinite(highValue)) {
                    highValue = Math.round(highValue);
                }
                if (isFinite(lowValue) && isFinite(highValue)) {
                    double integersInRange = highValue - lowValue + 1;
                    if (!isNaN(distinctValuesCount) && distinctValuesCount > integersInRange) {
                        distinctValuesCount = integersInRange;
                    }
                }
            }

            return VariableStatsEstimate.builder()
                    .setNullsFraction(sourceStats.getNullsFraction())
                    .setLowValue(lowValue)
                    .setHighValue(highValue)
                    .setDistinctValuesCount(distinctValuesCount)
                    .build();
        }

        private VariableStatsEstimate computeNegationStatistics(CallExpression call, Void context)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate stats = getSourceStats(call, context, 0);
            if (resolution.isNegateFunction(call.getFunctionHandle())) {
                return VariableStatsEstimate.buildFrom(stats)
                        .setLowValue(-stats.getHighValue())
                        .setHighValue(-stats.getLowValue())
                        .build();
            }
            throw new IllegalStateException(format("Unexpected sign: %s(%s)", call.getDisplayName(), call.getFunctionHandle()));
        }

        private VariableStatsEstimate computeHashCodeOperatorStatistics(CallExpression call, Void context)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate argStats = getSourceStats(call, context, 0);
            VariableStatsEstimate.Builder result =
                    VariableStatsEstimate.builder()
                            .setAverageRowSize(8.0)
                            .setNullsFraction(argStats.getNullsFraction())
                            .setDistinctValuesCount(minExcludingNaNs(argStats.getDistinctValuesCount(), input.getOutputRowCount()));
            return result.build();
        }

        private VariableStatsEstimate computeComparisonOperatorStatistics(CallExpression call, Void context)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate left = getSourceStats(call, context, 0);
            VariableStatsEstimate right = getSourceStats(call, context, 1);
            VariableStatsEstimate.Builder result =
                    VariableStatsEstimate.builder()
                            .setAverageRowSize(1.0)
                            .setNullsFraction(left.getNullsFraction() + right.getNullsFraction() - left.getNullsFraction() * right.getNullsFraction())
                            .setDistinctValuesCount(2.0);
            return result.build();
        }

        private VariableStatsEstimate computeArithmeticBinaryStatistics(CallExpression call, Void context)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate left = getSourceStats(call, context, 0);
            VariableStatsEstimate right = getSourceStats(call, context, 1);

            VariableStatsEstimate.Builder result = VariableStatsEstimate.builder()
                    .setAverageRowSize(maxExcludingNaNs(left.getAverageRowSize(), right.getAverageRowSize()))
                    .setNullsFraction(left.getNullsFraction() + right.getNullsFraction() - left.getNullsFraction() * right.getNullsFraction())
                    .setDistinctValuesCount(minExcludingNaNs(left.getDistinctValuesCount() * right.getDistinctValuesCount(), input.getOutputRowCount()));

            FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(call.getFunctionHandle());
            checkState(functionMetadata.getOperatorType().isPresent());
            OperatorType operatorType = functionMetadata.getOperatorType().get();
            double leftLow = left.getLowValue();
            double leftHigh = left.getHighValue();
            double rightLow = right.getLowValue();
            double rightHigh = right.getHighValue();
            if (isNaN(leftLow) || isNaN(leftHigh) || isNaN(rightLow) || isNaN(rightHigh)) {
                result.setLowValue(NaN).setHighValue(NaN);
            }
            else if (operatorType.equals(DIVIDE) && rightLow < 0 && rightHigh > 0) {
                result.setLowValue(Double.NEGATIVE_INFINITY)
                        .setHighValue(Double.POSITIVE_INFINITY);
            }
            else if (operatorType.equals(MODULUS)) {
                double maxDivisor = maxExcludingNaNs(abs(rightLow), abs(rightHigh));
                if (leftHigh <= 0) {
                    result.setLowValue(maxExcludingNaNs(-maxDivisor, leftLow))
                            .setHighValue(0);
                }
                else if (leftLow >= 0) {
                    result.setLowValue(0)
                            .setHighValue(minExcludingNaNs(maxDivisor, leftHigh));
                }
                else {
                    result.setLowValue(maxExcludingNaNs(-maxDivisor, leftLow))
                            .setHighValue(minExcludingNaNs(maxDivisor, leftHigh));
                }
            }
            else {
                double v1 = operate(operatorType, leftLow, rightLow);
                double v2 = operate(operatorType, leftLow, rightHigh);
                double v3 = operate(operatorType, leftHigh, rightLow);
                double v4 = operate(operatorType, leftHigh, rightHigh);
                double lowValue = minExcludingNaNs(v1, v2, v3, v4);
                double highValue = maxExcludingNaNs(v1, v2, v3, v4);

                result.setLowValue(lowValue)
                        .setHighValue(highValue);
            }

            return result.build();
        }

        private double operate(OperatorType operator, double left, double right)
        {
            switch (operator) {
                case ADD:
                    return left + right;
                case SUBTRACT:
                    return left - right;
                case MULTIPLY:
                    return left * right;
                case DIVIDE:
                    return left / right;
                case MODULUS:
                    return left % right;
                default:
                    throw new IllegalStateException("Unsupported ArithmeticBinaryExpression.Operator: " + operator);
            }
        }
    }

    private class ExpressionStatsVisitor
            extends AstVisitor<VariableStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;
        private final TypeProvider types;

        ExpressionStatsVisitor(PlanNodeStatsEstimate input, Session session, TypeProvider types)
        {
            this.input = input;
            this.session = session;
            this.types = types;
        }

        @Override
        protected VariableStatsEstimate visitNode(Node node, Void context)
        {
            return VariableStatsEstimate.unknown();
        }

        @Override
        protected VariableStatsEstimate visitSymbolReference(SymbolReference node, Void context)
        {
            return input.getVariableStatistics(new VariableReferenceExpression(getSourceLocation(node), node.getName(), types.get(node)));
        }

        @Override
        protected VariableStatsEstimate visitNullLiteral(NullLiteral node, Void context)
        {
            return nullStatsEstimate();
        }

        @Override
        protected VariableStatsEstimate visitLiteral(Literal node, Void context)
        {
            Object value = evaluate(metadata, session.toConnectorSession(), node);
            Type type = ExpressionAnalyzer.createConstantAnalyzer(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver(), session, ImmutableMap.of(), WarningCollector.NOOP).analyze(node, Scope.create());
            OptionalDouble doubleValue = toStatsRepresentation(metadata, session, type, value);
            VariableStatsEstimate.Builder estimate = VariableStatsEstimate.builder()
                    .setNullsFraction(0)
                    .setDistinctValuesCount(1);

            if (doubleValue.isPresent()) {
                estimate.setLowValue(doubleValue.getAsDouble());
                estimate.setHighValue(doubleValue.getAsDouble());
            }
            return estimate.build();
        }

        @Override
        protected VariableStatsEstimate visitFunctionCall(FunctionCall node, Void context)
        {
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, node, types);
            ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(node, metadata, session, expressionTypes);
            Object value = interpreter.optimize(NoOpVariableResolver.INSTANCE);

            if (value == null || value instanceof NullLiteral) {
                return nullStatsEstimate();
            }

            if (value instanceof Expression && !(value instanceof Literal)) {
                // value is not a constant
                return VariableStatsEstimate.unknown();
            }

            // value is a constant
            return VariableStatsEstimate.builder()
                    .setNullsFraction(0)
                    .setDistinctValuesCount(1)
                    .build();
        }

        private Map<NodeRef<Expression>, Type> getExpressionTypes(Session session, Expression expression, TypeProvider types)
        {
            ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                    metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver(),
                    session,
                    types,
                    emptyMap(),
                    node -> new IllegalStateException("Unexpected node: " + node),
                    WarningCollector.NOOP,
                    false);
            expressionAnalyzer.analyze(expression, Scope.create());
            return expressionAnalyzer.getExpressionTypes();
        }

        @Override
        protected VariableStatsEstimate visitCast(Cast node, Void context)
        {
            VariableStatsEstimate sourceStats = process(node.getExpression());
            TypeSignature targetType = TypeSignature.parseTypeSignature(node.getType());

            // todo - make this general postprocessing rule.
            double distinctValuesCount = sourceStats.getDistinctValuesCount();
            double lowValue = sourceStats.getLowValue();
            double highValue = sourceStats.getHighValue();

            if (TypeUtils.isIntegralType(targetType, metadata.getFunctionAndTypeManager())) {
                // todo handle low/high value changes if range gets narrower due to cast (e.g. BIGINT -> SMALLINT)
                if (isFinite(lowValue)) {
                    lowValue = Math.round(lowValue);
                }
                if (isFinite(highValue)) {
                    highValue = Math.round(highValue);
                }
                if (isFinite(lowValue) && isFinite(highValue)) {
                    double integersInRange = highValue - lowValue + 1;
                    if (!isNaN(distinctValuesCount) && distinctValuesCount > integersInRange) {
                        distinctValuesCount = integersInRange;
                    }
                }
            }

            return VariableStatsEstimate.builder()
                    .setNullsFraction(sourceStats.getNullsFraction())
                    .setLowValue(lowValue)
                    .setHighValue(highValue)
                    .setDistinctValuesCount(distinctValuesCount)
                    .build();
        }

        @Override
        protected VariableStatsEstimate visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            VariableStatsEstimate stats = process(node.getValue());
            switch (node.getSign()) {
                case PLUS:
                    return stats;
                case MINUS:
                    return VariableStatsEstimate.buildFrom(stats)
                            .setLowValue(-stats.getHighValue())
                            .setHighValue(-stats.getLowValue())
                            .build();
                default:
                    throw new IllegalStateException("Unexpected sign: " + node.getSign());
            }
        }

        @Override
        protected VariableStatsEstimate visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            requireNonNull(node, "node is null");
            VariableStatsEstimate left = process(node.getLeft());
            VariableStatsEstimate right = process(node.getRight());

            VariableStatsEstimate.Builder result = VariableStatsEstimate.builder()
                    .setAverageRowSize(maxExcludingNaNs(left.getAverageRowSize(), right.getAverageRowSize()))
                    .setNullsFraction(left.getNullsFraction() + right.getNullsFraction() - left.getNullsFraction() * right.getNullsFraction())
                    .setDistinctValuesCount(minExcludingNaNs(left.getDistinctValuesCount() * right.getDistinctValuesCount(), input.getOutputRowCount()));

            double leftLow = left.getLowValue();
            double leftHigh = left.getHighValue();
            double rightLow = right.getLowValue();
            double rightHigh = right.getHighValue();
            if (isNaN(leftLow) || isNaN(leftHigh) || isNaN(rightLow) || isNaN(rightHigh)) {
                result.setLowValue(NaN)
                        .setHighValue(NaN);
            }
            else if (node.getOperator() == ArithmeticBinaryExpression.Operator.DIVIDE && rightLow < 0 && rightHigh > 0) {
                result.setLowValue(Double.NEGATIVE_INFINITY)
                        .setHighValue(Double.POSITIVE_INFINITY);
            }
            else if (node.getOperator() == ArithmeticBinaryExpression.Operator.MODULUS) {
                double maxDivisor = maxExcludingNaNs(abs(rightLow), abs(rightHigh));
                if (leftHigh <= 0) {
                    result.setLowValue(maxExcludingNaNs(-maxDivisor, leftLow))
                            .setHighValue(0);
                }
                else if (leftLow >= 0) {
                    result.setLowValue(0)
                            .setHighValue(minExcludingNaNs(maxDivisor, leftHigh));
                }
                else {
                    result.setLowValue(maxExcludingNaNs(-maxDivisor, leftLow))
                            .setHighValue(minExcludingNaNs(maxDivisor, leftHigh));
                }
            }
            else {
                double v1 = operate(node.getOperator(), leftLow, rightLow);
                double v2 = operate(node.getOperator(), leftLow, rightHigh);
                double v3 = operate(node.getOperator(), leftHigh, rightLow);
                double v4 = operate(node.getOperator(), leftHigh, rightHigh);
                double lowValue = minExcludingNaNs(v1, v2, v3, v4);
                double highValue = maxExcludingNaNs(v1, v2, v3, v4);

                result.setLowValue(lowValue)
                        .setHighValue(highValue);
            }

            return result.build();
        }

        private double operate(ArithmeticBinaryExpression.Operator operator, double left, double right)
        {
            switch (operator) {
                case ADD:
                    return left + right;
                case SUBTRACT:
                    return left - right;
                case MULTIPLY:
                    return left * right;
                case DIVIDE:
                    return left / right;
                case MODULUS:
                    return left % right;
                default:
                    throw new IllegalStateException("Unsupported ArithmeticBinaryExpression.Operator: " + operator);
            }
        }

        @Override
        protected VariableStatsEstimate visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            requireNonNull(node, "node is null");
            VariableStatsEstimate result = null;
            for (Expression operand : node.getOperands()) {
                VariableStatsEstimate operandEstimates = process(operand);
                if (result != null) {
                    result = estimateCoalesce(input, result, operandEstimates);
                }
                else {
                    result = operandEstimates;
                }
            }
            return requireNonNull(result, "result is null");
        }
    }

    private static VariableStatsEstimate estimateCoalesce(PlanNodeStatsEstimate input, VariableStatsEstimate left, VariableStatsEstimate right)
    {
        // Question to reviewer: do you have a method to check if fraction is empty or saturated?
        if (left.getNullsFraction() == 0) {
            return left;
        }
        else if (left.getNullsFraction() == 1.0) {
            return right;
        }
        else {
            return VariableStatsEstimate.builder()
                    .setLowValue(minExcludingNaNs(left.getLowValue(), right.getLowValue()))
                    .setHighValue(maxExcludingNaNs(left.getHighValue(), right.getHighValue()))
                    .setDistinctValuesCount(left.getDistinctValuesCount() +
                            minExcludingNaNs(right.getDistinctValuesCount(), input.getOutputRowCount() * left.getNullsFraction()))
                    .setNullsFraction(left.getNullsFraction() * right.getNullsFraction())
                    // TODO check if dataSize estimation method is correct
                    .setAverageRowSize(maxExcludingNaNs(left.getAverageRowSize(), right.getAverageRowSize()))
                    .build();
        }
    }

    private static VariableStatsEstimate nullStatsEstimate()
    {
        return VariableStatsEstimate.builder()
                .setDistinctValuesCount(0)
                .setNullsFraction(1)
                .build();
    }
}
