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
package com.facebook.presto.sql;

import com.facebook.presto.sql.tree.Cube;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.GroupingElement;
import com.facebook.presto.sql.tree.GroupingSets;
import com.facebook.presto.sql.tree.LongLiteral;
import com.facebook.presto.sql.tree.Rollup;
import com.facebook.presto.sql.tree.SimpleGroupBy;
import com.facebook.presto.sql.tree.TemplateExpressionRewriter;
import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.collect.Iterables.getOnlyElement;

public class TemplateExpressionFormatter
        extends ExpressionFormatter
{
    private final ExpressionFormatter expressionFormatter = new ExpressionFormatter();
    @Override
    public String format(Expression expression, Optional<List<Expression>> parameters)
    {
        if (parameters.isPresent()) {
            // In a sql template, we want to replace all literals with parameters. Therefore, values should not be provided for processing parameters.
            throw new IllegalArgumentException("parameters cannot be present");
        }

        return new Formatter(parameters).process(TemplateExpressionRewriter.rewrite(expression), null);
    }

    @Override
    protected String formatGroupingSet(List<Expression> groupingSet, Optional<List<Expression>> parameters)
    {
        return String.format("(%s)", Joiner.on(", ").join(groupingSet.stream()
                .map(e -> {
                    if (e instanceof LongLiteral) {
                        return expressionFormatter.format(e, parameters);
                    }
                    else {
                        return format(e, parameters);
                    }
                })
                .iterator()));
    }

    @Override
    String formatGroupBy(List<GroupingElement> groupingElements)
    {
        ImmutableList.Builder<String> resultStrings = ImmutableList.builder();

        for (GroupingElement groupingElement : groupingElements) {
            String result = "";
            if (groupingElement instanceof SimpleGroupBy) {
                List<Expression> columns = ((SimpleGroupBy) groupingElement).getExpressions();
                if (columns.size() == 1) {
                    Expression column = getOnlyElement(columns);
                    if (column instanceof LongLiteral) {
                        result = expressionFormatter.format(getOnlyElement(columns), Optional.empty());
                    }
                    else {
                        result = format(getOnlyElement(columns), Optional.empty());
                    }
                }
                else {
                    result = formatGroupingSet(columns, Optional.empty());
                }
            }
            else if (groupingElement instanceof GroupingSets) {
                result = String.format("GROUPING SETS (%s)", Joiner.on(", ").join(
                        ((GroupingSets) groupingElement).getSets().stream()
                                .map(e -> formatGroupingSet(e, Optional.empty()))
                                .iterator()));
            }
            else if (groupingElement instanceof Cube) {
                result = String.format("CUBE %s", formatGroupingSet(((Cube) groupingElement).getExpressions(), Optional.empty()));
            }
            else if (groupingElement instanceof Rollup) {
                result = String.format("ROLLUP %s", formatGroupingSet(((Rollup) groupingElement).getExpressions(), Optional.empty()));
            }
            resultStrings.add(result);
        }
        return Joiner.on(", ").join(resultStrings.build());
    }

    @Override
    String formatLimit(String limit)
    {
        return "?";
    }
}
