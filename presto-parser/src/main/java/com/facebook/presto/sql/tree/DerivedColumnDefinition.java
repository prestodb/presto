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

import static com.google.common.base.Preconditions.checkArgument;

public class DerivedColumnDefinition
{
    private final Expression expression;
    private final boolean isPersistent;
    private final boolean isGeneratedAlways;
    private final String expressionAsString;

    public DerivedColumnDefinition(Expression expression, String expressionAsString, boolean isPersistent, boolean isGeneratedAlways)
    {
        checkArgument(isPersistent, "Virtual columns are not yet supported.");
        this.expressionAsString = expressionAsString;
        this.expression = expression;
        this.isPersistent = isPersistent;
        this.isGeneratedAlways = isGeneratedAlways;
    }

    public String getExpressionAsString()
    {
        return expressionAsString;
    }

    public Expression getExpression()
    {
        return expression;
    }
}
