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
package com.facebook.presto.analyzer.crux.tree;

import static java.util.Objects.requireNonNull;

public class LiteralExpression
        extends Expression
{
    private final Literal value;
    private final String text;

    public LiteralExpression(
            CodeLocation location,
            Type type,
            ExpressionCardinality cardinality,
            Literal value,
            String text)
    {
        super(Expression.Kind.LiteralExpression, location, type, cardinality);
        this.value = requireNonNull(value, "value is null");
        this.text = requireNonNull(text, "text is null");
    }

    public Literal getValue()
    {
        return value;
    }

    public String getText()
    {
        return text;
    }
}
