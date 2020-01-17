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

import com.facebook.presto.sql.tree.CurrentUser;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.QualifiedName;
import com.google.common.collect.ImmutableList;

public class DesugarCurrentUser
        extends ExpressionRewriteRuleSet
{
    public DesugarCurrentUser()
    {
        super(createRewrite());
    }

    private static ExpressionRewriter createRewrite()
    {
        return (expression, context) -> ExpressionTreeRewriter.rewriteWith(new com.facebook.presto.sql.tree.ExpressionRewriter<Void>()
        {
            @Override
            public Expression rewriteCurrentUser(CurrentUser node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                return DesugarCurrentUser.getCall(node);
            }
        }, expression);
    }

    public static FunctionCall getCall(CurrentUser node)
    {
        return new FunctionCall(QualifiedName.of("$current_user"), ImmutableList.of());
    }
}
