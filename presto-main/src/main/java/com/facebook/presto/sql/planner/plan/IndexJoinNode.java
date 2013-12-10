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
import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class IndexJoinNode
        extends PlanNode
{
    private final Type type;
    private final PlanNode probeSource;
    private final PlanNode indexSource;
    private final List<EquiJoinClause> criteria;

    @JsonCreator
    public IndexJoinNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("type") Type type,
            @JsonProperty("probeSource") PlanNode probeSource,
            @JsonProperty("indexSource") PlanNode indexSource,
            @JsonProperty("criteria") List<EquiJoinClause> criteria)
    {
        super(id);

        this.type = checkNotNull(type, "type is null");
        this.probeSource = checkNotNull(probeSource, "probeSource is null");
        this.indexSource = checkNotNull(indexSource, "indexSource is null");
        this.criteria = ImmutableList.copyOf(checkNotNull(criteria, "criteria is null"));
    }

    public enum Type
    {
        INNER("Inner"),
        SOURCE_OUTER("SourceOuter");

        private final String joinLabel;

        private Type(String joinLabel)
        {
            this.joinLabel = joinLabel;
        }

        public String getJoinLabel()
        {
            return joinLabel;
        }
    }

    @JsonProperty("type")
    public Type getType()
    {
        return type;
    }

    @JsonProperty("probeSource")
    public PlanNode getProbeSource()
    {
        return probeSource;
    }

    @JsonProperty("indexSource")
    public PlanNode getIndexSource()
    {
        return indexSource;
    }

    @JsonProperty("criteria")
    public List<EquiJoinClause> getCriteria()
    {
        return criteria;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(probeSource);
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(probeSource.getOutputSymbols())
                .addAll(indexSource.getOutputSymbols())
                .build();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitIndexJoin(this, context);
    }

    public static class EquiJoinClause
    {
        private final Symbol probe;
        private final Symbol index;

        @JsonCreator
        public EquiJoinClause(@JsonProperty("probe") Symbol probe, @JsonProperty("index") Symbol index)
        {
            this.probe = checkNotNull(probe, "probe is null");
            this.index = checkNotNull(index, "index is null");
        }

        @JsonProperty("probe")
        public Symbol getProbe()
        {
            return probe;
        }

        @JsonProperty("index")
        public Symbol getIndex()
        {
            return index;
        }

        public static Function<EquiJoinClause, Symbol> probeGetter()
        {
            return new Function<EquiJoinClause, Symbol>()
            {
                @Override
                public Symbol apply(EquiJoinClause input)
                {
                    return input.getProbe();
                }
            };
        }

        public static Function<EquiJoinClause, Symbol> indexGetter()
        {
            return new Function<EquiJoinClause, Symbol>()
            {
                @Override
                public Symbol apply(EquiJoinClause input)
                {
                    return input.getIndex();
                }
            };
        }
    }
}
