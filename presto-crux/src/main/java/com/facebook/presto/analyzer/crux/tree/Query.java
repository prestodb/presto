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

public class Query
        extends Statement
{
    private final Kind kind;

    public Query(Kind kind, CodeLocation location)
    {
        super(Statement.Kind.Query, location);
        this.kind = requireNonNull(kind, "query kind is null");
    }

    public Kind getQueryKind()
    {
        return this.kind;
    }

    public boolean isEmptyQuery()
    {
        return this instanceof EmptyQuery;
    }

    public EmptyQuery asEmptyQuery()
    {
        return (EmptyQuery) this;
    }

    public boolean isSelectQuery()
    {
        return this instanceof SelectQuery;
    }

    public SelectQuery asSelectQuery()
    {
        return (SelectQuery) this;
    }

    public enum Kind
    {
        EmptyQuery,
        SelectQuery
    }
}
