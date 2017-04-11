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
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

@Immutable
public class SemiJoinNode
        extends PlanNode
{
    private final PlanNode source;
    private final PlanNode filteringSource;
    private final Symbol sourceJoinSymbol;
    private final Symbol filteringSourceJoinSymbol;
    private final Symbol semiJoinOutput;
    private final Optional<Symbol> sourceHashSymbol;
    private final Optional<Symbol> filteringSourceHashSymbol;
    private final Optional<DistributionType> distributionType;

    @JsonCreator
    public SemiJoinNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("filteringSource") PlanNode filteringSource,
            @JsonProperty("sourceJoinSymbol") Symbol sourceJoinSymbol,
            @JsonProperty("filteringSourceJoinSymbol") Symbol filteringSourceJoinSymbol,
            @JsonProperty("semiJoinOutput") Symbol semiJoinOutput,
            @JsonProperty("sourceHashSymbol") Optional<Symbol> sourceHashSymbol,
            @JsonProperty("filteringSourceHashSymbol") Optional<Symbol> filteringSourceHashSymbol,
            @JsonProperty("distributionType") Optional<DistributionType> distributionType)
    {
        super(id);
        this.source = requireNonNull(source, "source is null");
        this.filteringSource = requireNonNull(filteringSource, "filteringSource is null");
        this.sourceJoinSymbol = requireNonNull(sourceJoinSymbol, "sourceJoinSymbol is null");
        this.filteringSourceJoinSymbol = requireNonNull(filteringSourceJoinSymbol, "filteringSourceJoinSymbol is null");
        this.semiJoinOutput = requireNonNull(semiJoinOutput, "semiJoinOutput is null");
        this.sourceHashSymbol = requireNonNull(sourceHashSymbol, "sourceHashSymbol is null");
        this.filteringSourceHashSymbol = requireNonNull(filteringSourceHashSymbol, "filteringSourceHashSymbol is null");
        this.distributionType = requireNonNull(distributionType, "distributionType is null");

        checkArgument(source.getOutputSymbols().contains(sourceJoinSymbol), "Source does not contain join symbol");
        checkArgument(filteringSource.getOutputSymbols().contains(filteringSourceJoinSymbol), "Filtering source does not contain filtering join symbol");
    }

    public enum DistributionType
    {
        PARTITIONED,
        REPLICATED
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty("filteringSource")
    public PlanNode getFilteringSource()
    {
        return filteringSource;
    }

    @JsonProperty("sourceJoinSymbol")
    public Symbol getSourceJoinSymbol()
    {
        return sourceJoinSymbol;
    }

    @JsonProperty("filteringSourceJoinSymbol")
    public Symbol getFilteringSourceJoinSymbol()
    {
        return filteringSourceJoinSymbol;
    }

    @JsonProperty("semiJoinOutput")
    public Symbol getSemiJoinOutput()
    {
        return semiJoinOutput;
    }

    @JsonProperty("sourceHashSymbol")
    public Optional<Symbol> getSourceHashSymbol()
    {
        return sourceHashSymbol;
    }

    @JsonProperty("filteringSourceHashSymbol")
    public Optional<Symbol> getFilteringSourceHashSymbol()
    {
        return filteringSourceHashSymbol;
    }

    @JsonProperty("distributionType")
    public Optional<DistributionType> getDistributionType()
    {
        return distributionType;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source, filteringSource);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(source.getOutputSymbols())
                .add(semiJoinOutput)
                .build();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitSemiJoin(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        checkArgument(newChildren.size() == 2, "expected newChildren to contain 2 nodes");
        return new SemiJoinNode(
                getId(),
                newChildren.get(0),
                newChildren.get(1),
                sourceJoinSymbol,
                filteringSourceJoinSymbol,
                semiJoinOutput,
                sourceHashSymbol,
                filteringSourceHashSymbol,
                distributionType);
    }
}
