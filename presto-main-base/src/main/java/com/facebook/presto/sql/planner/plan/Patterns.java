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

import com.facebook.presto.matching.Pattern;
import com.facebook.presto.matching.Property;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.CteConsumerNode;
import com.facebook.presto.spi.plan.CteProducerNode;
import com.facebook.presto.spi.plan.CteReferenceNode;
import com.facebook.presto.spi.plan.DeleteNode;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.IndexSourceNode;
import com.facebook.presto.spi.plan.IntersectNode;
import com.facebook.presto.spi.plan.JoinNode;
import com.facebook.presto.spi.plan.JoinType;
import com.facebook.presto.spi.plan.LimitNode;
import com.facebook.presto.spi.plan.MarkDistinctNode;
import com.facebook.presto.spi.plan.MaterializedViewScanNode;
import com.facebook.presto.spi.plan.OutputNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.plan.SemiJoinNode;
import com.facebook.presto.spi.plan.SortNode;
import com.facebook.presto.spi.plan.SpatialJoinNode;
import com.facebook.presto.spi.plan.TableFinishNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.TableWriterNode;
import com.facebook.presto.spi.plan.TopNNode;
import com.facebook.presto.spi.plan.UnionNode;
import com.facebook.presto.spi.plan.UnnestNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.plan.WindowNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.matching.Pattern.typeOf;
import static com.facebook.presto.matching.Property.optionalProperty;
import static com.facebook.presto.matching.Property.property;

public class Patterns
{
    private Patterns() {}

    public static Pattern<AssignUniqueId> assignUniqueId()
    {
        return typeOf(AssignUniqueId.class);
    }

    public static Pattern<AggregationNode> aggregation()
    {
        return typeOf(AggregationNode.class);
    }

    public static Pattern<GroupIdNode> groupId()
    {
        return typeOf(GroupIdNode.class);
    }

    public static Pattern<ApplyNode> applyNode()
    {
        return typeOf(ApplyNode.class);
    }

    public static Pattern<DeleteNode> delete()
    {
        return typeOf(DeleteNode.class);
    }
    public static Pattern<UpdateNode> update()
    {
        return typeOf(UpdateNode.class);
    }

    public static Pattern<ExchangeNode> exchange()
    {
        return typeOf(ExchangeNode.class);
    }

    public static Pattern<EnforceSingleRowNode> enforceSingleRow()
    {
        return typeOf(EnforceSingleRowNode.class);
    }

    public static Pattern<FilterNode> filter()
    {
        return typeOf(FilterNode.class);
    }

    public static Pattern<IndexSourceNode> indexSource()
    {
        return typeOf(IndexSourceNode.class);
    }

    public static Pattern<JoinNode> join()
    {
        return typeOf(JoinNode.class);
    }

    public static Pattern<SpatialJoinNode> spatialJoin()
    {
        return typeOf(SpatialJoinNode.class);
    }

    public static Pattern<LateralJoinNode> lateralJoin()
    {
        return typeOf(LateralJoinNode.class);
    }

    public static Pattern<OffsetNode> offset()
    {
        return typeOf(OffsetNode.class);
    }

    public static Pattern<LimitNode> limit()
    {
        return typeOf(LimitNode.class);
    }

    public static Pattern<MarkDistinctNode> markDistinct()
    {
        return typeOf(MarkDistinctNode.class);
    }

    public static Pattern<MaterializedViewScanNode> materializedViewScan()
    {
        return typeOf(MaterializedViewScanNode.class);
    }

    public static Pattern<OutputNode> output()
    {
        return typeOf(OutputNode.class);
    }

    public static Pattern<CteProducerNode> cteProducer()
    {
        return typeOf(CteProducerNode.class);
    }

    public static Pattern<CteConsumerNode> cteConsumer()
    {
        return typeOf(CteConsumerNode.class);
    }

    public static Pattern<CteReferenceNode> cteReference()
    {
        return typeOf(CteReferenceNode.class);
    }

    public static Pattern<SequenceNode> sequenceNode()
    {
        return typeOf(SequenceNode.class);
    }

    public static Pattern<ProjectNode> project()
    {
        return typeOf(ProjectNode.class);
    }

    public static Pattern<RemoteSourceNode> remoteSource()
    {
        return typeOf(RemoteSourceNode.class);
    }

    public static Pattern<SampleNode> sample()
    {
        return typeOf(SampleNode.class);
    }

    public static Pattern<SemiJoinNode> semiJoin()
    {
        return typeOf(SemiJoinNode.class);
    }

    public static Pattern<SortNode> sort()
    {
        return typeOf(SortNode.class);
    }

    public static Pattern<TableFinishNode> tableFinish()
    {
        return typeOf(TableFinishNode.class);
    }

    public static Pattern<TableScanNode> tableScan()
    {
        return typeOf(TableScanNode.class);
    }

    public static Pattern<TableWriterNode> tableWriterNode()
    {
        return typeOf(TableWriterNode.class);
    }

    public static Pattern<TableWriterMergeNode> tableWriterMergeNode()
    {
        return typeOf(TableWriterMergeNode.class);
    }

    public static Pattern<MergeWriterNode> mergeWriter()
    {
        return typeOf(MergeWriterNode.class);
    }

    public static Pattern<TopNNode> topN()
    {
        return typeOf(TopNNode.class);
    }

    public static Pattern<UnionNode> union()
    {
        return typeOf(UnionNode.class);
    }

    public static Pattern<IntersectNode> intersect()
    {
        return typeOf(IntersectNode.class);
    }

    public static Pattern<ValuesNode> values()
    {
        return typeOf(ValuesNode.class);
    }

    public static Pattern<WindowNode> window()
    {
        return typeOf(WindowNode.class);
    }

    public static Pattern<TableFunctionNode> tableFunction()
    {
        return typeOf(TableFunctionNode.class);
    }

    public static Pattern<TableFunctionProcessorNode> tableFunctionProcessor()
    {
        return typeOf(TableFunctionProcessorNode.class);
    }

    public static Pattern<RowNumberNode> rowNumber()
    {
        return typeOf(RowNumberNode.class);
    }

    public static Pattern<UnnestNode> unnest()
    {
        return typeOf(UnnestNode.class);
    }

    public static Property<PlanNode, PlanNode> source()
    {
        return optionalProperty("source", node -> node.getSources().size() == 1 ?
                Optional.of(node.getSources().get(0)) :
                Optional.empty());
    }

    public static Property<PlanNode, List<PlanNode>> sources()
    {
        return property("sources", PlanNode::getSources);
    }

    public static class Aggregation
    {
        public static Property<AggregationNode, List<VariableReferenceExpression>> groupingColumns()
        {
            return property("groupingKeys", AggregationNode::getGroupingKeys);
        }

        public static Property<AggregationNode, AggregationNode.Step> step()
        {
            return property("step", AggregationNode::getStep);
        }
    }

    public static class Apply
    {
        public static Property<ApplyNode, List<VariableReferenceExpression>> correlation()
        {
            return property("correlation", ApplyNode::getCorrelation);
        }
    }

    public static class Join
    {
        public static Property<JoinNode, JoinType> type()
        {
            return property("type", JoinNode::getType);
        }
    }

    public static class LateralJoin
    {
        public static Property<LateralJoinNode, List<VariableReferenceExpression>> correlation()
        {
            return property("correlation", LateralJoinNode::getCorrelation);
        }

        public static Property<LateralJoinNode, PlanNode> subquery()
        {
            return property("subquery", LateralJoinNode::getSubquery);
        }
    }

    public static class Limit
    {
        public static Property<LimitNode, Long> count()
        {
            return property("count", LimitNode::getCount);
        }
    }

    public static class Sample
    {
        public static Property<SampleNode, Double> sampleRatio()
        {
            return property("sampleRatio", SampleNode::getSampleRatio);
        }

        public static Property<SampleNode, SampleNode.Type> sampleType()
        {
            return property("sampleType", SampleNode::getSampleType);
        }
    }

    public static class TopN
    {
        public static Property<TopNNode, TopNNode.Step> step()
        {
            return property("step", TopNNode::getStep);
        }
    }

    public static class Values
    {
        public static Property<ValuesNode, List<List<RowExpression>>> rows()
        {
            return property("rows", ValuesNode::getRows);
        }
    }

    public static class Exchange
    {
        public static Property<ExchangeNode, ExchangeNode.Scope> scope()
        {
            return property("scope", ExchangeNode::getScope);
        }

        public static Property<ExchangeNode, ExchangeNode.Type> type()
        {
            return property("type", ExchangeNode::getType);
        }
    }
}
