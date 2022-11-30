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
package com.facebook.presto.analyzer.crux;

import com.facebook.presto.analyzer.crux.tree.EmptyQuery;
import com.facebook.presto.analyzer.crux.tree.Expression;
import com.facebook.presto.analyzer.crux.tree.LiteralExpression;
import com.facebook.presto.analyzer.crux.tree.Query;
import com.facebook.presto.analyzer.crux.tree.SelectItem;
import com.facebook.presto.analyzer.crux.tree.SelectQuery;
import com.facebook.presto.analyzer.crux.tree.SemanticTree;
import com.facebook.presto.analyzer.crux.tree.Statement;

/**
 * TODO: A dummy implementation of Crux analyzer visitor. This would be replaced with actual implementation.
 * At the moment this visitor only has limited classes as we need to enable development.
 */
public abstract class SemanticTreeVisitor
{
    public void visitEmptyQuery(EmptyQuery node)
    {
    }

    public void visitExpression(Expression node)
    {
        switch (node.getExpressionKind()) {
            case LiteralExpression:
                visitLiteralExpression(node.asLiteralExpression());
                break;
            default:
                throw new UnsupportedOperationException("Missing case: " + node.getExpressionKind().name());
        }
    }

    public void visitLiteralExpression(LiteralExpression node)
    {
    }

    public void visitQuery(Query node)
    {
        switch (node.getQueryKind()) {
            case EmptyQuery:
                visitEmptyQuery(node.asEmptyQuery());
                break;
            case SelectQuery:
                visitSelectQuery(node.asSelectQuery());
                break;
            default:
                throw new UnsupportedOperationException("Missing case: " + node.getQueryKind().name());
        }
    }

    public void visitSelectItem(SelectItem node)
    {
        visitExpression(node.getValue());
    }

    public void visitSelectQuery(SelectQuery node)
    {
        visitQuery(node.getQuery());
        for (SelectItem item : node.getSelectItems()) {
            visitSelectItem(item);
        }
    }

    public void visitSemanticTree(SemanticTree node)
    {
        switch (node.getSemanticTreeKind()) {
            case Expression:
                visitExpression(node.asExpression());
                break;
            case Statement:
                visitStatement(node.asStatement());
                break;
            default:
                throw new UnsupportedOperationException("Missing case: " + node.getSemanticTreeKind().name());
        }
    }

    public void visitStatement(Statement node)
    {
        switch (node.getStatementKind()) {
            case Query:
                visitQuery(node.asQuery());
                break;
            default:
                throw new UnsupportedOperationException("Missing case: " + node.getStatementKind().name());
        }
    }
}
