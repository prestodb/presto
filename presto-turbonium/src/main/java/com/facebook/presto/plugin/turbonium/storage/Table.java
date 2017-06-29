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
package com.facebook.presto.plugin.turbonium.storage;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.log.Logger;

import javax.annotation.concurrent.ThreadSafe;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class Table
{
    private static final Logger log = Logger.get(Table.class);
    private final List<Type> types;
    private final List<Column> columns;
    private final long sizeBytes;
    private final List<Long> columnSizes;

    private Table(List<Type> types, List<Column> columns)
    {
        checkArgument(!requireNonNull(types, "types is null").isEmpty(), "types is empty");
        this.types = types;
        checkArgument(!requireNonNull(columns, "columns is null").isEmpty(), "columns is empty");
        this.columns = columns;
        ImmutableList.Builder<Long> columnSizesBuilder = ImmutableList.builder();
        long sizeBytes = 0L;
        int columnIndex = 0;
        for (Column column : columns) {
            sizeBytes += column.getSizeBytes();
            columnSizesBuilder.add(column.getSizeBytes());
        }
        this.sizeBytes = sizeBytes;
        columnSizes = columnSizesBuilder.build();
    }

    public long getSizeBytes()
    {
        return sizeBytes;
    }

    public List<Long> getColumnSizes()
    {
        return columnSizes;
    }

    public long getRowCount()
    {
        return columns.get(0).getPositionCount();
    }

    public int getSegmentCount()
    {
        return columns.get(0).segmentCount();
    }

    public List<Page> getPages(int partNumber, int totalParts, List<Integer> columnIndexes, Map<Integer, Domain> effectivePredicate)
    {
        ImmutableList.Builder<Type> typesBuilder = ImmutableList.builder();
        for (int index = 0; index < columnIndexes.size(); index++) {
            typesBuilder.add(types.get(columnIndexes.get(index)));
        }
        List<Type> columnTypes = typesBuilder.build();
        PageListBuilder builder = new PageListBuilder(columnTypes);
        int segments = getSegmentCount();
        for (int segment = partNumber; segment < segments; segment += totalParts) {
            if (skipSegment(effectivePredicate, segment)) {
                continue;
            }
            int positions = columns.get(0).getSegment(segment).size();
            for (int position = 0; position < positions; position++) {
                builder.beginRow();
                for (int channel = 0; channel < columnIndexes.size(); channel++) {
                    int columnIndex = columnIndexes.get(channel);
                    BlockBuilder blockBuilder = builder.nextBlockBuilder();
                    Column column = columns.get(columnIndex);
                    column.getSegment(segment).write(blockBuilder, position);
                }
            }
        }
        return builder.build();
    }

    private boolean skipSegment(Map<Integer, Domain> effectivePredicate, int segment)
    {
        boolean skipSegment = false;
        for (Map.Entry<Integer, Domain> entry : effectivePredicate.entrySet()) {
            int columnIndex = entry.getKey();
            Domain predicateDomain = entry.getValue();
            Domain columnDomain = columns.get(columnIndex).getSegment(segment).getDomain();
            if (predicateDomain.isAll()) {
                continue;
            }
            else if (predicateDomain.isOnlyNull()) {
                if (!columnDomain.isNullAllowed()) {
                    skipSegment = true;
                    break;
                }
            }
            else if (predicateDomain.isNone()) {
                skipSegment = true;
                break;
            }
            else if (!predicateDomain.overlaps(columnDomain)) {
                skipSegment = true;
                break;
            }
        }
        return skipSegment;
    }

    public static Builder builder(List<Type> types, boolean disableEncoding)
    {
        return new Builder(types, disableEncoding);
    }

    @ThreadSafe
    public static class Builder
    {
        private final List<Type> types;
        private final List<ColumnBuilder> columnBuilders;

        private Builder(List<Type> types, boolean disableEncoding)
        {
            this.types = ImmutableList.copyOf(types);
            ImmutableList.Builder<ColumnBuilder> builder = ImmutableList.builder();
            for (int channel = 0; channel < types.size(); channel++) {
                builder.add(ColumnBuilder.create(channel, types.get(channel), disableEncoding));
            }
            this.columnBuilders = builder.build();
        }

        public synchronized void appendPage(Page page)
        {
            columnBuilders.parallelStream().forEach(builder -> builder.appendPage(page));
            /*for (int channel = 0; channel < types.size(); channel++) {
                columnBuilders.get(channel).appendPage(page);
            }*/
        }

        public synchronized Table build()
        {
            return new Table(types, columnBuilders.parallelStream().map(ColumnBuilder::build).collect(Collectors.toList()));
        }
    }
}
