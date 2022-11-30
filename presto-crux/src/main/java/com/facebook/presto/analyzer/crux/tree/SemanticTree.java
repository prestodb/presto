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
/**
 * TODO: A dummy implementation of Semantic tree. This would be replaced with actual implementation.
 */
public class SemanticTree
{
    private final Kind kind;
    private final CodeLocation location;

    public SemanticTree(Kind kind, CodeLocation location)
    {
        this.kind = requireNonNull(kind, "semanticTree kind is null");
        this.location = location;
    }

    public Kind getSemanticTreeKind()
    {
        return this.kind;
    }

    public CodeLocation getLocation()
    {
        return location;
    }

    public boolean isExpression()
    {
        return this instanceof Expression;
    }

    public Expression asExpression()
    {
        return (Expression) this;
    }

    public boolean isSelectItem()
    {
        return this instanceof SelectItem;
    }

    public SelectItem asSelectItem()
    {
        return (SelectItem) this;
    }

    public boolean isStatement()
    {
        return this instanceof Statement;
    }

    public Statement asStatement()
    {
        return (Statement) this;
    }

    public enum Kind
    {
        Expression,
        SelectItem,
        Statement
    }
}
