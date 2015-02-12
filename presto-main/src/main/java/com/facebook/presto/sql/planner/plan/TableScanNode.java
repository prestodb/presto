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

import com.facebook.presto.metadata.ColumnHandle;
import com.facebook.presto.metadata.Partition;
import com.facebook.presto.metadata.TableHandle;
import com.facebook.presto.spi.TupleDomain;
import com.facebook.presto.sql.planner.DomainUtils;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.facebook.presto.sql.planner.DomainUtils.columnHandleToSymbol;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@Immutable
public class TableScanNode
        extends PlanNode
{
    private final TableHandle table;
    private final List<Symbol> outputSymbols;
    private final Map<Symbol, ColumnHandle> assignments; // symbol -> column
    private final SummarizedPartition summarizedPartition;
    private final boolean partitionsDroppedBySerialization;

    // HACK!
    //
    // This field exists for the sole purpose of being able to print the original predicates (from the query) in
    // a human readable way. Predicates that get converted to and from TupleDomains might get more bulky and thus
    // more difficult to read when printed.
    // For example:
    // (ds > '2013-01-01') in the original query could easily become (ds IN ('2013-01-02', '2013-01-03', ...)) after the partitions are generated.
    // To make this work, the originalConstraint should be set exactly once after the first predicate push down and never adjusted after that.
    // In this way, we are always guaranteed to have a readable predicate that provides some kind of upper bound on the constraints.
    private final Expression originalConstraint;

    public TableScanNode(PlanNodeId id, TableHandle table, List<Symbol> outputSymbols, Map<Symbol, ColumnHandle> assignments, @Nullable Expression originalConstraint, Optional<GeneratedPartitions> generatedPartitions)
    {
        this(id, table, outputSymbols, assignments, originalConstraint, new SummarizedPartition(generatedPartitions), false);
    }

    public TableScanNode(PlanNodeId id, TableHandle table, List<Symbol> outputSymbols, Map<Symbol, ColumnHandle> assignments, @Nullable Expression originalConstraint, SummarizedPartition summarizedPartition)
    {
        this(id, table, outputSymbols, assignments, originalConstraint, summarizedPartition, false);
    }

    @JsonCreator
    public TableScanNode(@JsonProperty("id") PlanNodeId id,
            @JsonProperty("table") TableHandle table,
            @JsonProperty("outputSymbols") List<Symbol> outputSymbols,
            @JsonProperty("assignments") Map<Symbol, ColumnHandle> assignments,
            @JsonProperty("originalConstraint") @Nullable Expression originalConstraint)
    {
        this(id, table, outputSymbols, assignments, originalConstraint, new SummarizedPartition(Optional.empty()), true);
    }

    private TableScanNode(PlanNodeId id, TableHandle table, List<Symbol> outputSymbols, Map<Symbol, ColumnHandle> assignments, @Nullable Expression originalConstraint, SummarizedPartition summarizedPartition, boolean partitionsDroppedBySerialization)
    {
        super(id);

        checkNotNull(table, "table is null");
        checkNotNull(outputSymbols, "outputSymbols is null");
        checkNotNull(assignments, "assignments is null");
        checkArgument(assignments.keySet().containsAll(outputSymbols), "assignments does not cover all of outputSymbols");
        checkNotNull(summarizedPartition, "summarizedPartition is null");

        this.table = table;
        this.outputSymbols = ImmutableList.copyOf(outputSymbols);
        this.assignments = ImmutableMap.copyOf(assignments);
        this.originalConstraint = originalConstraint;
        this.summarizedPartition = summarizedPartition;
        this.partitionsDroppedBySerialization = partitionsDroppedBySerialization;
        checkArgument(summarizedPartition.getPartitionDomainSummary().isNone() || ImmutableSet.copyOf(assignments.values()).containsAll(summarizedPartition.getPartitionDomainSummary().getDomains().keySet()), "Assignments do not include all of the ColumnHandles specified by the Partitions");
    }

    @JsonProperty("table")
    public TableHandle getTable()
    {
        return table;
    }

    @Override
    @JsonProperty("outputSymbols")
    public List<Symbol> getOutputSymbols()
    {
        return outputSymbols;
    }

    @JsonProperty("assignments")
    public Map<Symbol, ColumnHandle> getAssignments()
    {
        return assignments;
    }

    @Nullable
    @JsonProperty("originalConstraint")
    public Expression getOriginalConstraint()
    {
        return originalConstraint;
    }

    public Optional<GeneratedPartitions> getGeneratedPartitions()
    {
        // If this exception throws, then we might want to consider making Partitions serializable by Jackson
        checkState(!partitionsDroppedBySerialization, "Can't access partitions after passing through serialization");
        return summarizedPartition.getGeneratedPartitions();
    }

    public TupleDomain<ColumnHandle> getPartitionsDomainSummary()
    {
        return summarizedPartition.getPartitionDomainSummary();
    }

    public SummarizedPartition getSummarizedPartition()
    {
        return summarizedPartition;
    }

    @JsonProperty("partitionDomainSummary")
    public String getPrintablePartitionDomainSummary()
    {
        // Since partitions are not serializable, we can provide an additional jackson field purely for information purposes (i.e. for logging)
        // If partitions ever become serializable, we can get rid of this method
        StringBuilder builder = new StringBuilder()
                .append("TupleDomain:");
        if (summarizedPartition.getPartitionDomainSummary().isAll()) {
            builder.append("ALL");
        }
        else if (summarizedPartition.getPartitionDomainSummary().isNone()) {
            builder.append("NONE");
        }
        else {
            builder.append(Maps.transformValues(columnHandleToSymbol(summarizedPartition.getPartitionDomainSummary().getDomains(), assignments), DomainUtils::simplifyDomain));
        }
        return builder.toString();
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitTableScan(this, context);
    }

    public static final class SummarizedPartition
    {
        private final Optional<GeneratedPartitions> generatedPartitions;
        private final TupleDomain<ColumnHandle> partitionDomainSummary;

        public SummarizedPartition(Optional<GeneratedPartitions> generatedPartitions)
        {
            this.generatedPartitions = checkNotNull(generatedPartitions, "generatedPartitions is null");
            this.partitionDomainSummary = computePartitionsDomainSummary(generatedPartitions);

        }

        private static TupleDomain<ColumnHandle> computePartitionsDomainSummary(Optional<GeneratedPartitions> generatedPartitions)
        {
            if (!generatedPartitions.isPresent()) {
                return TupleDomain.all();
            }

            if (generatedPartitions.get().getPartitions().isEmpty()) {
                return TupleDomain.none();
            }

            List<TupleDomain<ColumnHandle>> domains = FluentIterable.from(generatedPartitions.get().getPartitions()).transform(Partition::getTupleDomain).toList();
            return TupleDomain.columnWiseUnion(domains);
        }

        public TupleDomain<ColumnHandle> getPartitionDomainSummary()
        {
            return partitionDomainSummary;
        }

        public Optional<GeneratedPartitions> getGeneratedPartitions()
        {
            return generatedPartitions;
        }
    }

    public static final class GeneratedPartitions
    {
        private final TupleDomain<ColumnHandle> tupleDomainInput; // The TupleDomain used to generate the current list of Partitions
        private final List<Partition> partitions;

        public GeneratedPartitions(TupleDomain<ColumnHandle> tupleDomainInput, List<Partition> partitions)
        {
            this.tupleDomainInput = checkNotNull(tupleDomainInput, "tupleDomainInput is null");
            this.partitions = ImmutableList.copyOf(checkNotNull(partitions, "partitions is null"));
        }

        public TupleDomain<ColumnHandle> getTupleDomainInput()
        {
            return tupleDomainInput;
        }

        public List<Partition> getPartitions()
        {
            return partitions;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(tupleDomainInput, partitions);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (this == obj) {
                return true;
            }
            if (obj == null || getClass() != obj.getClass()) {
                return false;
            }
            GeneratedPartitions other = (GeneratedPartitions) obj;
            return Objects.equals(this.tupleDomainInput, other.tupleDomainInput) &&
                    Objects.equals(this.partitions, other.partitions);
        }
    }
}
