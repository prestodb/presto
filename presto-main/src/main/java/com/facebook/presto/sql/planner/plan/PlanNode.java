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
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

import java.util.List;

import static java.util.Objects.requireNonNull;

@JsonTypeInfo(
        use = JsonTypeInfo.Id.NAME,
        include = JsonTypeInfo.As.PROPERTY,
        property = "@type")
@JsonSubTypes({
        @JsonSubTypes.Type(value = OutputNode.class, name = "output"),
        @JsonSubTypes.Type(value = ProjectNode.class, name = "project"),
        @JsonSubTypes.Type(value = TableScanNode.class, name = "tablescan"),
        @JsonSubTypes.Type(value = ValuesNode.class, name = "values"),
        @JsonSubTypes.Type(value = AggregationNode.class, name = "aggregation"),
        @JsonSubTypes.Type(value = MarkDistinctNode.class, name = "markDistinct"),
        @JsonSubTypes.Type(value = FilterNode.class, name = "filter"),
        @JsonSubTypes.Type(value = WindowNode.class, name = "window"),
        @JsonSubTypes.Type(value = RowNumberNode.class, name = "rowNumber"),
        @JsonSubTypes.Type(value = TopNRowNumberNode.class, name = "topnRowNumber"),
        @JsonSubTypes.Type(value = LimitNode.class, name = "limit"),
        @JsonSubTypes.Type(value = DistinctLimitNode.class, name = "distinctlimit"),
        @JsonSubTypes.Type(value = TopNNode.class, name = "topn"),
        @JsonSubTypes.Type(value = SampleNode.class, name = "sample"),
        @JsonSubTypes.Type(value = SortNode.class, name = "sort"),
        @JsonSubTypes.Type(value = RemoteSourceNode.class, name = "remoteSource"),
        @JsonSubTypes.Type(value = JoinNode.class, name = "join"),
        @JsonSubTypes.Type(value = SemiJoinNode.class, name = "semijoin"),
        @JsonSubTypes.Type(value = IndexJoinNode.class, name = "indexjoin"),
        @JsonSubTypes.Type(value = IndexSourceNode.class, name = "indexsource"),
        @JsonSubTypes.Type(value = TableWriterNode.class, name = "tablewriter"),
        @JsonSubTypes.Type(value = DeleteNode.class, name = "delete"),
        @JsonSubTypes.Type(value = MetadataDeleteNode.class, name = "metadatadelete"),
        @JsonSubTypes.Type(value = TableFinishNode.class, name = "tablecommit"),
        @JsonSubTypes.Type(value = UnnestNode.class, name = "unnest"),
        @JsonSubTypes.Type(value = ExchangeNode.class, name = "exchange"),
        @JsonSubTypes.Type(value = UnionNode.class, name = "union"),
        @JsonSubTypes.Type(value = IntersectNode.class, name = "intersect"),
        @JsonSubTypes.Type(value = EnforceSingleRowNode.class, name = "scalar"),
        @JsonSubTypes.Type(value = GroupIdNode.class, name = "groupid"),
        @JsonSubTypes.Type(value = ExplainAnalyzeNode.class, name = "explainAnalyze"),
        @JsonSubTypes.Type(value = ApplyNode.class, name = "apply"),
        @JsonSubTypes.Type(value = AssignUniqueId.class, name = "assignUniqueId"),
})
public abstract class PlanNode
{
    private final PlanNodeId id;

    protected PlanNode(PlanNodeId id)
    {
        requireNonNull(id, "id is null");
        this.id = id;
    }

    @JsonProperty("id")
    public PlanNodeId getId()
    {
        return id;
    }

    public abstract List<PlanNode> getSources();

    public abstract List<Symbol> getOutputSymbols();

    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitPlan(this, context);
    }
}
