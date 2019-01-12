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
package io.prestosql.sql.planner.plan;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import io.prestosql.sql.planner.Symbol;

import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.concat;
import static java.util.Objects.requireNonNull;

@Immutable
public final class RowNumberNode
        extends PlanNode
{
    private final PlanNode source;
    private final List<Symbol> partitionBy;
    private final Optional<Integer> maxRowCountPerPartition;
    private final Symbol rowNumberSymbol;
    private final Optional<Symbol> hashSymbol;

    @JsonCreator
    public RowNumberNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("partitionBy") List<Symbol> partitionBy,
            @JsonProperty("rowNumberSymbol") Symbol rowNumberSymbol,
            @JsonProperty("maxRowCountPerPartition") Optional<Integer> maxRowCountPerPartition,
            @JsonProperty("hashSymbol") Optional<Symbol> hashSymbol)
    {
        super(id);

        requireNonNull(source, "source is null");
        requireNonNull(partitionBy, "partitionBy is null");
        requireNonNull(rowNumberSymbol, "rowNumberSymbol is null");
        requireNonNull(maxRowCountPerPartition, "maxRowCountPerPartition is null");
        requireNonNull(hashSymbol, "hashSymbol is null");

        this.source = source;
        this.partitionBy = ImmutableList.copyOf(partitionBy);
        this.rowNumberSymbol = rowNumberSymbol;
        this.maxRowCountPerPartition = maxRowCountPerPartition;
        this.hashSymbol = hashSymbol;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.copyOf(concat(source.getOutputSymbols(), ImmutableList.of(rowNumberSymbol)));
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public List<Symbol> getPartitionBy()
    {
        return partitionBy;
    }

    @JsonProperty
    public Symbol getRowNumberSymbol()
    {
        return rowNumberSymbol;
    }

    @JsonProperty
    public Optional<Integer> getMaxRowCountPerPartition()
    {
        return maxRowCountPerPartition;
    }

    @JsonProperty
    public Optional<Symbol> getHashSymbol()
    {
        return hashSymbol;
    }

    @Override
    public <R, C> R accept(PlanVisitor<R, C> visitor, C context)
    {
        return visitor.visitRowNumber(this, context);
    }

    @Override
    public PlanNode replaceChildren(List<PlanNode> newChildren)
    {
        return new RowNumberNode(getId(), Iterables.getOnlyElement(newChildren), partitionBy, rowNumberSymbol, maxRowCountPerPartition, hashSymbol);
    }
}
