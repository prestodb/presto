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
package io.prestosql.sql.planner.assertions;

import com.google.common.collect.ImmutableList;
import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.ApplyNode;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.planner.plan.ProjectNode;
import io.prestosql.sql.tree.Expression;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;
import static io.prestosql.sql.ExpressionUtils.rewriteIdentifiersToSymbolReferences;
import static java.util.Objects.requireNonNull;

public class ExpressionMatcher
        implements RvalueMatcher
{
    private final String sql;
    private final Expression expression;

    public ExpressionMatcher(String expression)
    {
        this.sql = requireNonNull(expression);
        this.expression = expression(requireNonNull(expression));
    }

    private Expression expression(String sql)
    {
        SqlParser parser = new SqlParser();
        return rewriteIdentifiersToSymbolReferences(parser.createExpression(sql));
    }

    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        Optional<Symbol> result = Optional.empty();
        ImmutableList.Builder<Expression> matchesBuilder = ImmutableList.builder();
        Map<Symbol, Expression> assignments = getAssignments(node);

        if (assignments == null) {
            return result;
        }

        ExpressionVerifier verifier = new ExpressionVerifier(symbolAliases);

        for (Map.Entry<Symbol, Expression> assignment : assignments.entrySet()) {
            if (verifier.process(assignment.getValue(), expression)) {
                result = Optional.of(assignment.getKey());
                matchesBuilder.add(assignment.getValue());
            }
        }

        List<Expression> matches = matchesBuilder.build();
        checkState(matches.size() < 2, "Ambiguous expression %s matches multiple assignments", expression,
                (matches.stream().map(Expression::toString).collect(Collectors.joining(", "))));
        return result;
    }

    private static Map<Symbol, Expression> getAssignments(PlanNode node)
    {
        if (node instanceof ProjectNode) {
            ProjectNode projectNode = (ProjectNode) node;
            return projectNode.getAssignments().getMap();
        }
        else if (node instanceof ApplyNode) {
            ApplyNode applyNode = (ApplyNode) node;
            return applyNode.getSubqueryAssignments().getMap();
        }
        else {
            return null;
        }
    }

    @Override
    public String toString()
    {
        return sql;
    }
}
