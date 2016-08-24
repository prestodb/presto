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
package com.facebook.presto.sql.tree;

/**
 * When walking Expressions, don't traverse into SubqueryExpressions
 */
public abstract class DefaultExpressionTraversalVisitor<R, C>
        extends DefaultTraversalVisitor<R, C>
{
    @Override
    protected R visitSubqueryExpression(SubqueryExpression node, C context)
    {
        // Don't traverse into Subqueries within an Expression
        return null;
    }

    @Override
    protected R visitExists(ExistsPredicate node, C context)
    {
        // Don't traverse into Subqueries within an Expression
        return null;
    }
}
