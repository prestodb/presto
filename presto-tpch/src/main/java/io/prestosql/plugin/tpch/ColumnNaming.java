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
package io.prestosql.plugin.tpch;

import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchEntity;

import java.util.function.Function;

public enum ColumnNaming
{
    STANDARD(TpchColumn::getColumnName),
    SIMPLIFIED(TpchColumn::getSimplifiedColumnName);

    private final Function<TpchColumn<?>, String> columnNameGetter;

    ColumnNaming(Function<TpchColumn<?>, String> columnNameGetter)
    {
        this.columnNameGetter = columnNameGetter;
    }

    public String getName(TpchColumn<? extends TpchEntity> tpchColumn)
    {
        return columnNameGetter.apply(tpchColumn);
    }
}
