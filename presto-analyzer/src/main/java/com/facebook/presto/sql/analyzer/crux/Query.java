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
package com.facebook.presto.sql.analyzer.crux;

import static java.util.Objects.requireNonNull;

public class Query
        extends Statement
{
    private final QueryKind queryKind;
    private final QuerySchema schema;

    protected Query(QueryKind queryKind, CodeLocation location, QuerySchema schema)
    {
        super(StatementKind.QUERY, location);
        this.queryKind = requireNonNull(queryKind, "queryKind is null");
        this.schema = requireNonNull(schema, "schema is null");
    }

    public QueryKind getQueryKind()
    {
        return queryKind;
    }

    public QuerySchema getSchema()
    {
        return schema;
    }

    public boolean isEmpty()
    {
        return this instanceof EmptyQuery;
    }

    public EmptyQuery asEmpty()
    {
        return (EmptyQuery) this;
    }

    public boolean isSelect()
    {
        return this instanceof SelectQuery;
    }

    public SelectQuery asSelect()
    {
        return (SelectQuery) this;
    }

    public boolean isDataSet()
    {
        return this instanceof DataSet;
    }

    public DataSet asDataSet()
    {
        return (DataSet) this;
    }

    public boolean isAlias()
    {
        return this instanceof AliasQuery;
    }

    public AliasQuery asAlias()
    {
        return (AliasQuery) this;
    }
}
