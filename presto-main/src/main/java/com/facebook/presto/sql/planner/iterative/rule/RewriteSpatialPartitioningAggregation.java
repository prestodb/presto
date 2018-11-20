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

import com.facebook.presto.matching.Captures;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.Signature;
import com.facebook.presto.spi.type.TypeSignature;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.AggregationNode.Aggregation;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.Map;

import static com.facebook.presto.SystemSessionProperties.getHashPartitionCount;
import static com.facebook.presto.metadata.FunctionKind.AGGREGATE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.plan.Patterns.aggregation;
import static com.google.common.collect.Iterables.getOnlyElement;
import static java.util.Objects.requireNonNull;

/**
 * Re-writes spatial_partitioning(geometry) aggregations into spatial_partitioning(envelope, partition_count)
 * on top of ST_Envelope(geometry) projection, e.g.
 *
 * - Aggregation: spatial_partitioning(geometry)
 *    - source
 *
 * becomes
 *
 * - Aggregation: spatial_partitioning(envelope, partition_count)
 *    - Project: envelope := ST_Envelope(geometry)
 *        - source
 *
 * , where partition_count is the value of session property hash_partition_count
 */
public class RewriteSpatialPartitioningAggregation
        implements Rule<AggregationNode>
{
    private static final TypeSignature GEOMETRY_TYPE_SIGNATURE = parseTypeSignature("Geometry");
    private static final String NAME = "spatial_partitioning";
    private static final Signature INTERNAL_SIGNATURE = new Signature(NAME, AGGREGATE, VARCHAR.getTypeSignature(), GEOMETRY_TYPE_SIGNATURE, INTEGER.getTypeSignature());
    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .matching(RewriteSpatialPartitioningAggregation::hasSpatialPartitioningAggregation);

    private final Metadata metadata;

    public RewriteSpatialPartitioningAggregation(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
    }

    private static boolean hasSpatialPartitioningAggregation(AggregationNode aggregation)
    {
        return aggregation.getAggregations().values().stream()
                .map(Aggregation::getCall)
                .anyMatch(call -> call.getName().toString().equals(NAME) && call.getArguments().size() == 1);
    }

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode node, Captures captures, Context context)
    {
        ImmutableMap.Builder<Symbol, Aggregation> aggregations = ImmutableMap.builder();
        Symbol partitionCountSymbol = context.getSymbolAllocator().newSymbol("partition_count", INTEGER);
        ImmutableMap.Builder<Symbol, Expression> envelopeAssignments = ImmutableMap.builder();
        for (Map.Entry<Symbol, Aggregation> entry : node.getAggregations().entrySet()) {
            Aggregation aggregation = entry.getValue();
            FunctionCall call = aggregation.getCall();
            QualifiedName name = call.getName();
            if (name.toString().equals(NAME) && call.getArguments().size() == 1) {
                Expression geometry = getOnlyElement(call.getArguments());
                Symbol envelopeSymbol = context.getSymbolAllocator().newSymbol("envelope", metadata.getType(GEOMETRY_TYPE_SIGNATURE));
                if (geometry instanceof FunctionCall && ((FunctionCall) geometry).getName().toString().equalsIgnoreCase("ST_Envelope")) {
                    envelopeAssignments.put(envelopeSymbol, geometry);
                }
                else {
                    envelopeAssignments.put(envelopeSymbol, new FunctionCall(QualifiedName.of("ST_Envelope"), ImmutableList.of(geometry)));
                }
                aggregations.put(entry.getKey(),
                        new Aggregation(
                                new FunctionCall(name, ImmutableList.of(envelopeSymbol.toSymbolReference(), partitionCountSymbol.toSymbolReference())),
                                INTERNAL_SIGNATURE,
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
                                        .putIdentities(node.getSource().getOutputSymbols())
                                        .put(partitionCountSymbol, new LongLiteral(Integer.toString(getHashPartitionCount(context.getSession()))))
                                        .putAll(envelopeAssignments.build())
                                        .build()),
                    aggregations.build(),
                    node.getGroupingSets(),
                    node.getPreGroupedSymbols(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol()));
    }
}
