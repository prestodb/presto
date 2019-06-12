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

import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.planner.RowExpressionInterpreter;
import com.facebook.presto.sql.planner.iterative.Rule;
import com.facebook.presto.sql.planner.plan.AggregationNode;

import static com.facebook.presto.sql.relational.Expressions.constant;

public class SimplifyRowExpressions
        extends RowExpressionRewriteRuleSet
{
    public SimplifyRowExpressions(Metadata metadata)
    {
        super(createRewriter(metadata));
    }

    private static RowExpressionRewriteRuleSet.RowExpressionRewriter createRewriter(Metadata metadata)
    {
        return new RowExpressionRewriter()
        {
            @Override
            public RowExpression rewrite(RowExpression expression, Rule.Context context)
            {
                RowExpressionInterpreter interpreter = new RowExpressionInterpreter(expression, metadata, context.getSession().toConnectorSession(), true);
                Object result = interpreter.optimize();
                if (result instanceof RowExpression) {
                    return (RowExpression) result;
                }
                return constant(result, expression.getType());
            }

            @Override
            public AggregationNode.Aggregation rewrite(AggregationNode.Aggregation aggregation, Rule.Context context)
            {
                // Do not evaluate aggregation
                return aggregation;
            }
        };
    }
}
