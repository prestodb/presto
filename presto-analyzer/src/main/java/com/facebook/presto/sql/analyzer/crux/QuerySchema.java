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

import java.util.ArrayList;
import java.util.List;

import static java.util.Objects.requireNonNull;

public class QuerySchema
{
    private final List<ColumnSpecification> columns;

    public QuerySchema()
    {
        this.columns = new ArrayList<>();
    }

    public QuerySchema(List<ColumnSpecification> columns)
    {
        this.columns = requireNonNull(columns, "columns is null");
    }

    public static QuerySchema fromSelectItems(List<SelectItem> items)
    {
        return new QuerySchema(selectItemsToColumnSpecs(items));
    }

    private static List<ColumnSpecification> selectItemsToColumnSpecs(List<SelectItem> items)
    {
        List<ColumnSpecification> columns = new ArrayList<>();
        int index = 0;
        for (SelectItem item : items) {
            List<String> names = item.getNames();

            columns.add(
                    index,
                    new ColumnSpecification(
                            index,
                            (names == null || names.size() == 0) ? null : names.get(0),
                            item.getValue().getType(),
                            false,
                            null,
                            false,
                            false));

            index += 1;
        }
        return columns;
    }

    public List<ColumnSpecification> getColumns()
    {
        return columns;
    }
}
