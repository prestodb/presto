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
package com.facebook.presto.metadata;

import com.facebook.presto.spi.relation.RowExpression;

import static java.util.Objects.requireNonNull;

public class PushdownFilterResult
{
    private final TableLayout layout;
    private final RowExpression unenforcedFilter;

    public PushdownFilterResult(TableLayout layout, RowExpression unenforcedFilter)
    {
        this.layout = requireNonNull(layout, "layout is null");
        this.unenforcedFilter = requireNonNull(unenforcedFilter, "unenforcedFilter is null");
    }

    public TableLayout getLayout()
    {
        return layout;
    }

    public RowExpression getUnenforcedFilter()
    {
        return unenforcedFilter;
    }
}
