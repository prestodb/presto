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
package com.facebook.presto.tpch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.Split;
import com.google.common.collect.ImmutableList;
import io.airlift.tpch.TpchColumn;
import io.airlift.tpch.TpchEntity;
import io.airlift.tpch.TpchTable;

import java.util.List;

import static com.facebook.presto.tpch.TpchRecordSet.createTpchRecordSet;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class TpchRecordSetProvider
        implements ConnectorRecordSetProvider
{
    @Override
    public boolean canHandle(Split split)
    {
        return split instanceof TpchSplit;
    }

    @Override
    public RecordSet getRecordSet(Split split, List<? extends ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkArgument(split instanceof TpchSplit, "Split must be a tpch split!");

        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "must provide at least one column");

        TpchSplit tpchSplit = (TpchSplit) split;
        String tableName = tpchSplit.getTableHandle().getTableName();

        TpchTable<?> tpchTable = TpchTable.getTable(tableName);

        return getRecordSet(tpchTable, columns, tpchSplit);
    }

    private <E extends TpchEntity> RecordSet getRecordSet(TpchTable<E> table, List<? extends ColumnHandle> columns, TpchSplit tpchSplit)
    {
        ImmutableList.Builder<TpchColumn<E>> builder = ImmutableList.builder();
        for (ColumnHandle column : columns) {
            checkArgument(column instanceof TpchColumnHandle, "column must be of type TpchColumnHandle, not %s", column.getClass().getName());
            String columnName = ((TpchColumnHandle) column).getColumnName();
            builder.add(table.getColumn(columnName));
        }

        return createTpchRecordSet(table, builder.build(), tpchSplit.getTableHandle().getScaleFactor(), tpchSplit.getPartNumber() + 1, tpchSplit.getTotalParts());
    }
}
