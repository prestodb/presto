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

import java.util.List;

import static java.util.Objects.requireNonNull;

public class SelectQuery
        extends Query
{
    private final Query query;
    private final List<SelectItem> selectItems;

    public SelectQuery(CodeLocation location, Query query, List<SelectItem> selectItems)
    {
        super(Kind.SelectQuery, location);
        this.query = requireNonNull(query, "query is null");
        this.selectItems = requireNonNull(selectItems, "selectItems is null");
    }

    public Query getQuery()
    {
        return query;
    }

    public List<SelectItem> getSelectItems()
    {
        return selectItems;
    }
}
