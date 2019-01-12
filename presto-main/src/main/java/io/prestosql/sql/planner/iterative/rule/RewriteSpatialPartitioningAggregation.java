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
package io.prestosql.sql.planner.iterative.rule;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Metadata;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.type.TypeSignature;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.AggregationNode.Aggregation;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.LongLiteral;
import io.prestosql.sql.tree.QualifiedName;

import java.util.Map;

import static com.google.common.collect.Iterables.getOnlyElement;
import static io.prestosql.SystemSessionProperties.getHashPartitionCount;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.spi.type.VarcharType.VARCHAR;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static java.util.Objects.requireNonNull;

/**
 * Re-writes spatial_partitioning(geometry) aggregations into spatial_partitioning(envelope, partition_count)
 * on top of ST_Envelope(geometry) projection, e.g.
 * <pre>
 * - Aggregation: spatial_partitioning(geometry)
 *    - source
 * </pre>
 * becomes
 * <pre>
 * - Aggregation: spatial_partitioning(envelope, partition_count)
 *    - Project: envelope := ST_Envelope(geometry)
 *        - source
 * </pre>
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
