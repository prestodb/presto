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
import com.facebook.presto.cost.StatsProvider;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.DoubleLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GenericLiteral;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.StringLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.sql.planner.assertions.MatchResult.NO_MATCH;
import static com.facebook.presto.sql.planner.assertions.MatchResult.match;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.castToExpression;
import static com.facebook.presto.sql.relational.OriginalExpressionUtils.isExpression;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.util.Objects.requireNonNull;

public class ValuesMatcher
        implements Matcher
{
    private final Map<String, Integer> outputSymbolAliases;
    private final Optional<Integer> expectedOutputSymbolCount;
    private final Optional<Set<List<Expression>>> expectedRows;

    public ValuesMatcher(
            Map<String, Integer> outputSymbolAliases,
            Optional<Integer> expectedOutputSymbolCount,
            Optional<List<List<Expression>>> expectedRows)
    {
        this.outputSymbolAliases = ImmutableMap.copyOf(outputSymbolAliases);
        this.expectedOutputSymbolCount = requireNonNull(expectedOutputSymbolCount, "expectedOutputSymbolCount is null");
        this.expectedRows = requireNonNull(expectedRows, "expectedRows is null").map(ImmutableSet::copyOf);
    }

    @Override
    public boolean shapeMatches(PlanNode node)
    {
        return (node instanceof ValuesNode) &&
                expectedOutputSymbolCount.map(Integer.valueOf(node.getOutputVariables().size())::equals).orElse(true);
    }

    @Override
    public MatchResult detailMatches(PlanNode node, StatsProvider stats, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        checkState(shapeMatches(node), "Plan testing framework error: shapeMatches returned false in detailMatches in %s", this.getClass().getName());
        ValuesNode valuesNode = (ValuesNode) node;

        if (!expectedRows.map(rows -> rows.equals(valuesNode.getRows()
                .stream()
                .map(rowExpressions -> rowExpressions.stream()
                        .map(rowExpression -> {
                            if (isExpression(rowExpression)) {
                                return castToExpression(rowExpression);
                            }
                            ConstantExpression expression = (ConstantExpression) rowExpression;
                            if (expression.getType().getJavaType() == boolean.class) {
                                return new BooleanLiteral(String.valueOf(expression.getValue()));
                            }
                            if (expression.getType().getJavaType() == long.class) {
                                return new LongLiteral(String.valueOf(expression.getValue()));
                            }
                            if (expression.getType().getJavaType() == double.class) {
                                return new DoubleLiteral(String.valueOf(expression.getValue()));
                            }
                            if (expression.getType().getJavaType() == Slice.class) {
                                return new StringLiteral(((Slice) expression.getValue()).toStringUtf8());
                            }
                            return new GenericLiteral(expression.getType().toString(), String.valueOf(expression.getValue()));
                        })
                        .collect(toImmutableList()))
                .collect(toImmutableSet())))
                .orElse(true)) {
            return NO_MATCH;
        }

        return match(SymbolAliases.builder()
                .putAll(Maps.transformValues(outputSymbolAliases, index -> new SymbolReference(valuesNode.getOutputVariables().get(index).getName())))
                .build());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .omitNullValues()
                .add("outputSymbolAliases", outputSymbolAliases)
                .add("expectedOutputSymbolCount", expectedOutputSymbolCount)
                .add("expectedRows", expectedRows)
                .toString();
    }
}
