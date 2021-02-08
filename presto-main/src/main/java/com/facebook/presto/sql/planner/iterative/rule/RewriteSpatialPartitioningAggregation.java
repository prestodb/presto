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
package com.facebook.presto.sql.planner.iterative.rule;

import com.facebook.presto.common.QualifiedObjectName;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.AggregationNode.Aggregation;
import com.facebook.presto.spi.plan.Assignments;
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.metadata.BuiltInTypeAndFunctionNamespaceManager.DEFAULT_NAMESPACE;
import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static com.facebook.presto.sql.planner.plan.AssignmentUtils.identitiesAsSymbolReferences;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.asSymbolReference;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToRowExpression;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

/**
 * Re-writes spatial_partitioning(geometry) aggregations into spatial_partitioning(envelope, partition_count)
 * on top of ST_Envelope(geometry) projection, e.g.
 * <p>
 * - Aggregation: spatial_partitioning(geometry)
 * - source
 * <p>
 * becomes
 * <p>
 * - Aggregation: spatial_partitioning(envelope, partition_count)
 * - Project: envelope := ST_Envelope(geometry)
 * - source
 * <p>
 * , where partition_count is the value of session property hash_partition_count
 */
public class RewriteSpatialPartitioningAggregation
        implements Rule<AggregationNode>
{
    private static final TypeSignature GEOMETRY_TYPE_SIGNATURE = parseTypeSignature("Geometry");
    private static final QualifiedObjectName NAME = QualifiedObjectName.valueOf(DEFAULT_NAMESPACE, "spatial_partitioning");
    private final Pattern<AggregationNode> pattern = aggregation().matching(this::hasSpatialPartitioningAggregation);

    private final Metadata metadata;

    public RewriteSpatialPartitioningAggregation(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    private boolean hasSpatialPartitioningAggregation(AggregationNode aggregationNode)
    {
        return aggregationNode.getAggregations().values().stream().anyMatch(
                aggregation -> metadata.getFunctionAndTypeManager().getFunctionMetadata(aggregation.getFunctionHandle()).getName().equals(NAME)
                        && aggregation.getArguments().size() == 1);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return pattern;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        ImmutableMap.Builder<VariableReferenceExpression, Aggregation> aggregations = ImmutableMap.builder();
        VariableReferenceExpression partitionCountVariable = context.getVariableAllocator().newVariable("partition_count", INTEGER);
        ImmutableMap.Builder<VariableReferenceExpression, RowExpression> envelopeAssignments = ImmutableMap.builder();
        for (Map.Entry<VariableReferenceExpression, Aggregation> entry : node.getAggregations().entrySet()) {
            Aggregation aggregation = entry.getValue();
            QualifiedObjectName name = metadata.getFunctionAndTypeManager().getFunctionMetadata(aggregation.getFunctionHandle()).getName();
            Type geometryType = metadata.getType(GEOMETRY_TYPE_SIGNATURE);
            if (name.equals(NAME) && aggregation.getArguments().size() == 1) {
                RowExpression geometry = getOnlyElement(aggregation.getArguments());
                VariableReferenceExpression envelopeVariable = context.getVariableAllocator().newVariable("envelope", geometryType);
                if (isFunctionNameMatch(geometry, "ST_Envelope")) {
                    envelopeAssignments.put(envelopeVariable, geometry);
                }
                else {
                    envelopeAssignments.put(envelopeVariable, castToRowExpression(new FunctionCall(QualifiedName.of("ST_Envelope"), ImmutableList.of(castToExpression(geometry)))));
                }
                aggregations.put(entry.getKey(),
                        new Aggregation(
                                new CallExpression(
                                        name.getObjectName(),
                                        metadata.getFunctionAndTypeManager().lookupFunction(NAME.getObjectName(), fromTypes(geometryType, INTEGER)),
                                        entry.getKey().getType(),
                                        ImmutableList.of(
                                                castToRowExpression(asSymbolReference(envelopeVariable)),
                                                castToRowExpression(asSymbolReference(partitionCountVariable)))),
                                Optional.empty(),
                                Optional.empty(),
                                false,
                                aggregation.getMask()));
            }
            else {
                aggregations.put(entry);
            }
        }

        return Result.ofPlanNode(
                new AggregationNode(
                        node.getId(),
                        new ProjectNode(
                                context.getIdAllocator().getNextId(),
                                node.getSource(),
                                Assignments.builder()
                                        .putAll(identitiesAsSymbolReferences(node.getSource().getOutputVariables()))
                                        .put(partitionCountVariable, castToRowExpression(new LongLiteral(Integer.toString(getHashPartitionCount(context.getSession())))))
                                        .putAll(envelopeAssignments.build())
                                        .build()),
                        aggregations.build(),
                        node.getGroupingSets(),
                        node.getPreGroupedVariables(),
                        node.getStep(),
                        node.getHashVariable(),
                        node.getGroupIdVariable()));
    }

    private static boolean isFunctionNameMatch(RowExpression rowExpression, String expectedName)
    {
        if (castToExpression(rowExpression) instanceof FunctionCall) {
            return ((FunctionCall) castToExpression(rowExpression)).getName().toString().equalsIgnoreCase(expectedName);
        }
        return false;
    }
}
