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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.tree.NullLiteral;
import com.google.common.collect.ImmutableList;

import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

public class NullExpressionMatcher
        extends ExpressionMatcher
{
    private static final NullLiteral NULL_LITERAL = new NullLiteral();
    private final Type type;

    public NullExpressionMatcher(Type type)
    {
        super(NULL_LITERAL);
        this.type = type;
    }

    @Override
    public Optional<VariableReferenceExpression> getAssignedVariable(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        Optional<VariableReferenceExpression> result = Optional.empty();
        ImmutableList.Builder<RowExpression> matchesBuilder = ImmutableList.builder();
        Map<VariableReferenceExpression, RowExpression> assignments = getAssignments(node);

        if (assignments == null) {
            return result;
        }

        for (Map.Entry<VariableReferenceExpression, RowExpression> assignment : assignments.entrySet()) {
            RowExpression rightValue = assignment.getValue();
            RowExpressionVerifier verifier = new RowExpressionVerifier(symbolAliases, metadata, session);
            if (rightValue.getType() == this.type && verifier.process(NULL_LITERAL, rightValue)) {
                result = Optional.of(assignment.getKey());
                matchesBuilder.add(rightValue);
            }
        }

        Set<RowExpression> matches = new HashSet<>(matchesBuilder.build());
        checkState(matches.size() < 2, "Ambiguous expression %s[%s] matches multiple assignments %s", NULL_LITERAL, type,
                (matches.stream().map(RowExpression::toString).collect(Collectors.joining(", "))));
        return result;
    }
}
