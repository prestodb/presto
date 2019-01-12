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
import io.prestosql.matching.Capture;
import io.prestosql.matching.Captures;
import io.prestosql.matching.Pattern;
import io.prestosql.metadata.Signature;
import io.prestosql.spi.type.StandardTypes;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.iterative.Rule;
import io.prestosql.sql.planner.plan.AggregationNode;
import io.prestosql.sql.planner.plan.Assignments;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.Expression;
import io.prestosql.sql.tree.FunctionCall;
import io.prestosql.sql.tree.Literal;
import io.prestosql.sql.tree.NullLiteral;
import io.prestosql.sql.tree.QualifiedName;
import io.prestosql.sql.tree.SymbolReference;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static io.prestosql.matching.Capture.newCapture;
import static io.prestosql.metadata.FunctionKind.AGGREGATE;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;
import static io.prestosql.sql.planner.plan.Patterns.aggregation;
import static io.prestosql.sql.planner.plan.Patterns.project;
import static io.prestosql.sql.planner.plan.Patterns.source;

public class SimplifyCountOverConstant
        implements Rule<AggregationNode>
{
    private static final Capture<ProjectNode> CHILD = newCapture();

    private static final Pattern<AggregationNode> PATTERN = aggregation()
            .with(source().matching(project().capturedAs(CHILD)));

    @Override
    public Pattern<AggregationNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Result apply(AggregationNode parent, Captures captures, Context context)
    {
        ProjectNode child = captures.get(CHILD);

        boolean changed = false;
        Map<Symbol, AggregationNode.Aggregation> aggregations = new LinkedHashMap<>(parent.getAggregations());

        for (Entry<Symbol, AggregationNode.Aggregation> entry : parent.getAggregations().entrySet()) {
            Symbol symbol = entry.getKey();
            AggregationNode.Aggregation aggregation = entry.getValue();

            if (isCountOverConstant(aggregation, child.getAssignments())) {
                changed = true;
                aggregations.put(symbol, new AggregationNode.Aggregation(
                        new FunctionCall(QualifiedName.of("count"), ImmutableList.of()),
                        new Signature("count", AGGREGATE, parseTypeSignature(StandardTypes.BIGINT)),
                        aggregation.getMask()));
            }
        }

        if (!changed) {
            return Result.empty();
        }

        return Result.ofPlanNode(new AggregationNode(
                parent.getId(),
                child,
                aggregations,
                parent.getGroupingSets(),
                ImmutableList.of(),
                parent.getStep(),
                parent.getHashSymbol(),
                parent.getGroupIdSymbol()));
    }

    private static boolean isCountOverConstant(AggregationNode.Aggregation aggregation, Assignments inputs)
    {
        Signature signature = aggregation.getSignature();
        if (!signature.getName().equals("count") || signature.getArgumentTypes().size() != 1) {
            return false;
        }

        Expression argument = aggregation.getCall().getArguments().get(0);
        if (argument instanceof SymbolReference) {
            argument = inputs.get(Symbol.from(argument));
        }

        return argument instanceof Literal && !(argument instanceof NullLiteral);
    }
}
