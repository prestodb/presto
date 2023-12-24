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

import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VariableWidthType;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeId;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.statistics.CostBasedSourceInfo;
import com.facebook.presto.spi.statistics.Estimate;
import com.facebook.presto.spi.statistics.JoinNodeStatistics;
import com.facebook.presto.spi.statistics.PartialAggregationStatistics;
import com.facebook.presto.spi.statistics.PlanStatistics;
import com.facebook.presto.spi.statistics.PlanStatisticsWithSourceInfo;
import com.facebook.presto.spi.statistics.SourceInfo;
import com.facebook.presto.spi.statistics.TableWriterNodeStatistics;
import com.facebook.presto.sql.Serialization;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.ImmutableMap;
import org.pcollections.HashTreePMap;
import org.pcollections.PMap;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import static com.facebook.presto.util.MoreMath.firstNonNaN;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Double.NaN;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;

public class PlanNodeStatsEstimate
{
    private static final double DEFAULT_DATA_SIZE_PER_COLUMN = 50;
    private static final PlanNodeStatsEstimate UNKNOWN = new PlanNodeStatsEstimate(NaN, NaN, false, ImmutableMap.of(), JoinNodeStatsEstimate.unknown(), TableWriterNodeStatsEstimate.unknown(), PartialAggregationStatsEstimate.unknown());

    private final double outputRowCount;
    private final double totalSize;
    private final PMap<VariableReferenceExpression, VariableStatsEstimate> variableStatistics;

    private final SourceInfo sourceInfo;

    private final JoinNodeStatsEstimate joinNodeStatsEstimate;

    private final TableWriterNodeStatsEstimate tableWriterNodeStatsEstimate;

    private final PartialAggregationStatsEstimate partialAggregationStatsEstimate;

    public static PlanNodeStatsEstimate unknown()
    {
        return UNKNOWN;
    }

    @JsonCreator
    public PlanNodeStatsEstimate(
            @JsonProperty("outputRowCount") double outputRowCount,
            @JsonProperty("totalSize") double totalSize,
            @JsonProperty("confident") boolean confident,
            @JsonProperty("variableStatistics") Map<VariableReferenceExpression, VariableStatsEstimate> variableStatistics,
            @JsonProperty("joinNodeStatsEstimate") JoinNodeStatsEstimate joinNodeStatsEstimate,
            @JsonProperty("tableWriterNodeStatsEstimate") TableWriterNodeStatsEstimate tableWriterNodeStatsEstimate,
            @JsonProperty("partialAggregationStatsEstimate") PartialAggregationStatsEstimate partialAggregationStatsEstimate)
    {
        this(outputRowCount,
                totalSize,
                HashTreePMap.from(requireNonNull(variableStatistics, "variableStatistics is null")),
                new CostBasedSourceInfo(confident), joinNodeStatsEstimate, tableWriterNodeStatsEstimate, partialAggregationStatsEstimate);
    }

    private PlanNodeStatsEstimate(double outputRowCount, double totalSize, boolean confident, PMap<VariableReferenceExpression, VariableStatsEstimate> variableStatistics)
    {
        this(outputRowCount, totalSize, variableStatistics, new CostBasedSourceInfo(confident));
    }

    public PlanNodeStatsEstimate(double outputRowCount, double totalSize, PMap<VariableReferenceExpression, VariableStatsEstimate> variableStatistics, SourceInfo sourceInfo)
    {
        this(outputRowCount, totalSize, variableStatistics, sourceInfo, JoinNodeStatsEstimate.unknown(), TableWriterNodeStatsEstimate.unknown(), PartialAggregationStatsEstimate.unknown());
    }

    public PlanNodeStatsEstimate(double outputRowCount, double totalSize, PMap<VariableReferenceExpression, VariableStatsEstimate> variableStatistics, SourceInfo sourceInfo,
            JoinNodeStatsEstimate joinNodeStatsEstimate, TableWriterNodeStatsEstimate tableWriterNodeStatsEstimate, PartialAggregationStatsEstimate partialAggregationStatsEstimate)
    {
        checkArgument(isNaN(outputRowCount) || outputRowCount >= 0, "outputRowCount cannot be negative");
        this.outputRowCount = outputRowCount;
        this.totalSize = totalSize;
        this.variableStatistics = variableStatistics;
        this.sourceInfo = requireNonNull(sourceInfo, "SourceInfo is null");
        this.joinNodeStatsEstimate = requireNonNull(joinNodeStatsEstimate, "joinNodeSpecificStatsEstimate is null");
        this.tableWriterNodeStatsEstimate = requireNonNull(tableWriterNodeStatsEstimate, "tableWriterNodeStatsEstimate is null");
        this.partialAggregationStatsEstimate = requireNonNull(partialAggregationStatsEstimate, "partialAggregationStatsEstimate is null");
    }

    /**
     * Returns estimated number of rows.
     * Unknown value is represented by {@link Double#NaN}
     */
    @JsonProperty
    public double getOutputRowCount()
    {
        return outputRowCount;
    }

    @JsonProperty
    public double getTotalSize()
    {
        return totalSize;
    }

    @JsonProperty
    public boolean isConfident()
    {
        return sourceInfo.isConfident();
    }

    public SourceInfo getSourceInfo()
    {
        return sourceInfo;
    }

    @JsonProperty
    public JoinNodeStatsEstimate getJoinNodeStatsEstimate()
    {
        return joinNodeStatsEstimate;
    }

    @JsonProperty
    public TableWriterNodeStatsEstimate getTableWriterNodeStatsEstimate()
    {
        return tableWriterNodeStatsEstimate;
    }

    @JsonProperty
    public PartialAggregationStatsEstimate getPartialAggregationStatsEstimate()
    {
        return partialAggregationStatsEstimate;
    }

    /**
     * Only use when getting all columns and meanwhile do not want to
     * do per-column estimation.
     */
    public double getOutputSizeInBytes()
    {
        return totalSize;
    }

    /**
     * Returns estimated data size.
     * Unknown value is represented by {@link Double#NaN}
     */
    public double getOutputSizeInBytes(PlanNode planNode)
    {
        requireNonNull(planNode, "planNode is null");

        if (!sourceInfo.estimateSizeUsingVariables() && !isNaN(totalSize)) {
            return totalSize;
        }

        return getOutputSizeForVariables(planNode.getOutputVariables());
    }

    /**
     * Returns estimated data size for given variables.
     * Unknown value is represented by {@link Double#NaN}
     */
    public double getOutputSizeForVariables(Collection<VariableReferenceExpression> outputVariables)
    {
        requireNonNull(outputVariables, "outputSymbols is null");

        return outputVariables.stream()
                .mapToDouble(variable -> getOutputSizeForVariable(getVariableStatistics(variable), variable.getType()))
                .sum();
    }

    private double getOutputSizeForVariable(VariableStatsEstimate variableStatistics, Type type)
    {
        checkArgument(type != null, "type is null");

        double averageRowSize = variableStatistics.getAverageRowSize();
        double nullsFraction = firstNonNaN(variableStatistics.getNullsFraction(), 0d);
        double numberOfNonNullRows = outputRowCount * (1.0 - nullsFraction);

        if (isNaN(averageRowSize)) {
            if (type instanceof FixedWidthType) {
                averageRowSize = ((FixedWidthType) type).getFixedSize();
            }
            else {
                averageRowSize = DEFAULT_DATA_SIZE_PER_COLUMN;
            }
        }

        double outputSize = numberOfNonNullRows * averageRowSize;

        // account for "is null" boolean array
        outputSize += outputRowCount * Byte.BYTES;

        // account for offsets array for variable width types
        if (type instanceof VariableWidthType) {
            outputSize += outputRowCount * Integer.BYTES;
        }

        return outputSize;
    }

    public PlanNodeStatsEstimate mapOutputRowCount(Function<Double, Double> mappingFunction)
    {
        return buildFrom(this).setOutputRowCount(mappingFunction.apply(outputRowCount)).build();
    }

    public PlanNodeStatsEstimate mapVariableColumnStatistics(VariableReferenceExpression variable, Function<VariableStatsEstimate, VariableStatsEstimate> mappingFunction)
    {
        return buildFrom(this)
                .addVariableStatistics(variable, mappingFunction.apply(getVariableStatistics(variable)))
                .build();
    }

    public VariableStatsEstimate getVariableStatistics(VariableReferenceExpression variable)
    {
        return variableStatistics.getOrDefault(variable, VariableStatsEstimate.unknown());
    }

    @JsonSerialize(keyUsing = Serialization.VariableReferenceExpressionSerializer.class)
    @JsonProperty
    public Map<VariableReferenceExpression, VariableStatsEstimate> getVariableStatistics()
    {
        return variableStatistics;
    }

    public Set<VariableReferenceExpression> getVariablesWithKnownStatistics()
    {
        return variableStatistics.keySet();
    }

    public boolean isOutputRowCountUnknown()
    {
        return isNaN(outputRowCount);
    }

    public boolean isTotalSizeUnknown()
    {
        return isNaN(totalSize);
    }

    public PlanNodeStatsEstimate combineStats(PlanStatistics planStatistics, SourceInfo statsSourceInfo)
    {
        if (planStatistics.getConfidence() > 0) {
            return new PlanNodeStatsEstimate(
                    planStatistics.getRowCount().getValue(),
                    planStatistics.getOutputSize().getValue(),
                    variableStatistics,
                    statsSourceInfo,
                    planStatistics.getJoinNodeStatistics().isEmpty() ? getJoinNodeStatsEstimate() :
                            new JoinNodeStatsEstimate(
                                    planStatistics.getJoinNodeStatistics().getNullJoinBuildKeyCount().getValue(),
                                    planStatistics.getJoinNodeStatistics().getJoinBuildKeyCount().getValue(),
                                    planStatistics.getJoinNodeStatistics().getNullJoinProbeKeyCount().getValue(),
                                    planStatistics.getJoinNodeStatistics().getJoinProbeKeyCount().getValue()),
                    planStatistics.getTableWriterNodeStatistics().isEmpty() ? getTableWriterNodeStatsEstimate() :
                            new TableWriterNodeStatsEstimate(planStatistics.getTableWriterNodeStatistics().getTaskCountIfScaledWriter().getValue()),
                    planStatistics.getPartialAggregationStatistics().isEmpty() ? getPartialAggregationStatsEstimate() :
                            new PartialAggregationStatsEstimate(planStatistics.getPartialAggregationStatistics().getPartialAggregationInputBytes().getValue(),
                                    planStatistics.getPartialAggregationStatistics().getPartialAggregationOutputBytes().getValue(),
                                    planStatistics.getPartialAggregationStatistics().getPartialAggregationInputRows().getValue(),
                                    planStatistics.getPartialAggregationStatistics().getPartialAggregationOutputRows().getValue()));
        }
        return this;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("outputRowCount", outputRowCount)
                .add("totalSize", totalSize)
                .add("variableStatistics", variableStatistics)
                .add("sourceInfo", sourceInfo)
                .add("joinNodeSpecificStatsEstimate", joinNodeStatsEstimate)
                .toString();
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
        PlanNodeStatsEstimate that = (PlanNodeStatsEstimate) o;
        return Double.compare(outputRowCount, that.outputRowCount) == 0 &&
                Double.compare(totalSize, that.totalSize) == 0 &&
                Objects.equals(variableStatistics, that.variableStatistics) &&
                Objects.equals(sourceInfo, that.sourceInfo) &&
                Objects.equals(joinNodeStatsEstimate, that.joinNodeStatsEstimate);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(outputRowCount, totalSize, variableStatistics, sourceInfo, joinNodeStatsEstimate);
    }

    public PlanStatisticsWithSourceInfo toPlanStatisticsWithSourceInfo(PlanNodeId id)
    {
        return new PlanStatisticsWithSourceInfo(
                id,
                new PlanStatistics(
                        Estimate.estimateFromDouble(outputRowCount),
                        Estimate.estimateFromDouble(totalSize),
                        sourceInfo.isConfident() ? 1 : 0,
                        new JoinNodeStatistics(
                                Estimate.estimateFromDouble(joinNodeStatsEstimate.getNullJoinBuildKeyCount()),
                                Estimate.estimateFromDouble(joinNodeStatsEstimate.getJoinBuildKeyCount()),
                                Estimate.estimateFromDouble(joinNodeStatsEstimate.getNullJoinProbeKeyCount()),
                                Estimate.estimateFromDouble(joinNodeStatsEstimate.getJoinProbeKeyCount())),
                        new TableWriterNodeStatistics(Estimate.estimateFromDouble(tableWriterNodeStatsEstimate.getTaskCountIfScaledWriter())),
                        PartialAggregationStatistics.empty()),
                sourceInfo);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    // Do not propagate the totalSize by default, because otherwise people have to explicitly set it to NaN or a not NaN but possibly wrong totalSize value is carried.
    // Given that we are only using this field for "leaf" simple join plans for now, it is safer to set it NaN by default so that if "accidentally" fetch the totalSize
    // at other places we can tell that it is not usable via isNaN(). Ideally, when we have implemented how to handle the totalSize field for all types of operator rules,
    // we should propagate totalSize as default to simplify the relevant operations in rules that do not change this field.
    public static Builder buildFrom(PlanNodeStatsEstimate other)
    {
        return new Builder(other.getOutputRowCount(), NaN, other.isConfident(), other.variableStatistics);
    }

    public static final class Builder
    {
        private double outputRowCount;
        private double totalSize;
        private boolean confident;
        private PMap<VariableReferenceExpression, VariableStatsEstimate> variableStatistics;
        private PartialAggregationStatsEstimate partialAggregationStatsEstimate;

        public Builder()
        {
            this(NaN, NaN, false, HashTreePMap.empty());
        }

        private Builder(double outputRowCount, double totalSize, boolean confident, PMap<VariableReferenceExpression, VariableStatsEstimate> variableStatistics)
        {
            this.outputRowCount = outputRowCount;
            this.totalSize = totalSize;
            this.confident = confident;
            this.variableStatistics = variableStatistics;
            this.partialAggregationStatsEstimate = PartialAggregationStatsEstimate.unknown();
        }

        public Builder setOutputRowCount(double outputRowCount)
        {
            this.outputRowCount = outputRowCount;
            return this;
        }

        public Builder setTotalSize(double totalSize)
        {
            this.totalSize = totalSize;
            return this;
        }

        public Builder setConfident(boolean confident)
        {
            this.confident = confident;
            return this;
        }

        public Builder setPartialAggregationStatsEstimate(PartialAggregationStatsEstimate partialAggregationStatsEstimate)
        {
            this.partialAggregationStatsEstimate = partialAggregationStatsEstimate;
            return this;
        }

        public Builder addVariableStatistics(VariableReferenceExpression variable, VariableStatsEstimate statistics)
        {
            variableStatistics = variableStatistics.plus(variable, statistics);
            return this;
        }

        public Builder addVariableStatistics(Map<VariableReferenceExpression, VariableStatsEstimate> variableStatistics)
        {
            this.variableStatistics = this.variableStatistics.plusAll(variableStatistics);
            return this;
        }

        public Builder removeVariableStatistics(VariableReferenceExpression variable)
        {
            variableStatistics = variableStatistics.minus(variable);
            return this;
        }

        public PlanNodeStatsEstimate build()
        {
            return new PlanNodeStatsEstimate(outputRowCount,
                    totalSize,
                    confident,
                    variableStatistics,
                    JoinNodeStatsEstimate.unknown(),
                    TableWriterNodeStatsEstimate.unknown(),
                    partialAggregationStatsEstimate);
        }
    }
}
