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

public class Statement
        extends SemanticTree
{
    private final Kind kind;

    public Statement(Kind kind, CodeLocation location)
    {
        super(SemanticTree.Kind.Statement, location);
        this.kind = requireNonNull(kind, "kind is null");
    }

    public Kind getStatementKind()
    {
        return this.kind;
    }

    public boolean isQuery()
    {
        return this instanceof Query;
    }

    public Query asQuery()
    {
        return (Query) this;
    }

    public enum Kind
    {
        Query
    }
}
