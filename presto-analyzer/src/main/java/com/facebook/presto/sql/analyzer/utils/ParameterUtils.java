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
package com.facebook.presto.sql.analyzer.utils;

import com.facebook.presto.sql.analyzer.MultiLineParameters;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.NodeLocation;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.Parameter;
import com.facebook.presto.sql.tree.Row;
import com.facebook.presto.sql.tree.Statement;
import com.google.common.collect.ImmutableList;

import java.util.Comparator;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class ParameterUtils
{
    private ParameterUtils() {}

    public static MultiLineParameters parameterExtractor(Statement statement, List<Expression> parameters)
    {
        List<NodeRef<Parameter>> refsList = ParameterExtractor.getParameters(statement).stream()
                .sorted(Comparator.comparing(
                        parameter -> parameter.getLocation().get(),
                        Comparator.comparing(NodeLocation::getLineNumber)
                                .thenComparing(NodeLocation::getColumnNumber)))
                .map(NodeRef::of)
                .collect(toImmutableList());

        ImmutableList.Builder<List<Expression>> parameterExpressions = ImmutableList.builder();
        if (!parameters.isEmpty() && parameters.get(0) instanceof Row) {
            for (Expression rowExpression : parameters) {
                ImmutableList.Builder<Expression> builder = ImmutableList.builder();
                List<Expression> exprs = ((Row) rowExpression).getItems();
                for (Expression expr : exprs) {
                    builder.add(expr);
                }
                parameterExpressions.add(builder.build());
            }
        }
        else {
            for (Expression expr : parameters) {
                parameterExpressions.add(ImmutableList.of(expr));
            }
        }
        return new MultiLineParameters(refsList, parameterExpressions.build());
    }
}
