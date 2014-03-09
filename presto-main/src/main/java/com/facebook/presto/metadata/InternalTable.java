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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.operator.Page;
import com.facebook.presto.operator.PageBuilder;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.block.BlockIterables.createBlockIterable;
import static com.facebook.presto.block.BlockUtils.emptyBlockIterable;
import static com.facebook.presto.type.Types.fromColumnType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class InternalTable
{
    private final Map<String, BlockIterable> columns;

    public InternalTable(Map<String, BlockIterable> columns)
    {
        this.columns = ImmutableMap.copyOf(checkNotNull(columns, "columns is null"));
    }

    public Set<String> getColumnNames()
    {
        return columns.keySet();
    }

    public BlockIterable getColumn(String columnName)
    {
        return columns.get(columnName);
    }

    public List<BlockIterable> getColumns(List<String> columnNames)
    {
        ImmutableList.Builder<BlockIterable> columns = ImmutableList.builder();
        for (String columnName : columnNames) {
            columns.add(getColumn(columnName));
        }
        return columns.build();
    }

    public static Builder builder(ColumnMetadata... columns)
    {
        return builder(ImmutableList.copyOf(columns));
    }

    public static Builder builder(List<ColumnMetadata> columns)
    {
        ImmutableList.Builder<String> names = ImmutableList.builder();
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (ColumnMetadata column : columns) {
            names.add(column.getName());
            types.add(fromColumnType(column.getType()));
        }
        return new Builder(types.build(), names.build());
    }

    public static class Builder
    {
        private final List<Type> types;
        private final List<String> columnNames;
        private final List<List<Block>> columns;
        private PageBuilder pageBuilder;

        public Builder(List<Type> types, List<String> columnNames)
        {
            this.types = ImmutableList.copyOf(checkNotNull(types, "types is null"));
            this.columnNames = ImmutableList.copyOf(checkNotNull(columnNames, "columnNames is null"));
            checkArgument(columnNames.size() == types.size(),
                    "Column name count does not match type count: columnNames=%s, types=%s", columnNames, types.size());

            columns = new ArrayList<>();
            for (int i = 0; i < types.size(); i++) {
                columns.add(new ArrayList<Block>());
            }

            pageBuilder = new PageBuilder(types);
        }

        public List<Type> getTypes()
        {
            return types;
        }

        public Builder add(Object... values)
        {
            for (int i = 0; i < types.size(); i++) {
                pageBuilder.getBlockBuilder(i).appendObject(values[i]);
            }

            if (pageBuilder.isFull()) {
                flushPage();
                pageBuilder.reset();
            }
            return this;
        }

        public InternalTable build()
        {
            flushPage();
            ImmutableMap.Builder<String, BlockIterable> data = ImmutableMap.builder();
            for (int i = 0; i < columns.size(); i++) {
                List<Block> column = columns.get(i);
                data.put(columnNames.get(i), column.isEmpty() ? emptyBlockIterable() : createBlockIterable(column));
            }
            return new InternalTable(data.build());
        }

        private void flushPage()
        {
            if (!pageBuilder.isEmpty()) {
                Page page = pageBuilder.build();
                for (int i = 0; i < types.size(); i++) {
                    columns.get(i).add(page.getBlock(i));
                }
            }
        }
    }
}
