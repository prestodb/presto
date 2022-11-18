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

public class ColumnReferenceExpression
        extends Expression
{
    private final Query query;
    private final int index;

    public ColumnReferenceExpression(CodeLocation location, Query query, int index)
    {
        super(ExpressionKind.COLUMN_REFERENCE, location, query.getSchema().getColumns().get(index).getType());
        this.query = requireNonNull(query, "query is null");
        this.index = index;
    }

    public Query getQuery()
    {
        return query;
    }

    public int getIndex()
    {
        return index;
    }
}
