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
package com.facebook.presto.util;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.serde.BlocksFileEncoding;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.tpch.TpchBlocksProvider;
import com.facebook.presto.tpch.TpchColumnHandle;
import com.facebook.presto.tpch.TpchRecordSet;
import com.facebook.presto.tpch.TpchTableHandle;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import com.google.common.base.Optional;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import io.airlift.tpch.TpchTable;
import io.airlift.units.DataSize;

import javax.inject.Inject;

import java.util.Iterator;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class InMemoryTpchBlocksProvider
        implements TpchBlocksProvider
{
    private final Map<String, RecordSet> data;

    @Inject
    public InMemoryTpchBlocksProvider()
    {
        this(0.01);
    }

    public InMemoryTpchBlocksProvider(double scaleFactor)
    {
        ImmutableMap.Builder<String, RecordSet> data = ImmutableMap.builder();
        for (TpchTable<?> table : TpchTable.getTables()) {
            data.put(table.getTableName(), TpchRecordSet.createTpchRecordSet(table, scaleFactor));
        }
        this.data = data.build();
    }

    @Override
    public BlockIterable getBlocks(TpchTableHandle tableHandle,
            TpchColumnHandle columnHandle,
            int partNumber,
            int totalParts,
            BlocksFileEncoding encoding)
    {
        return new TpchBlockIterable(Type.fromColumnType(columnHandle.getType()),
                partNumber,
                totalParts,
                tableHandle.getTableName(),
                columnHandle.getFieldIndex());
    }

    private class TpchBlockIterable
            implements BlockIterable
    {
        private final TupleInfo.Type fieldType;
        private final String tableName;
        private final int fieldIndex;
        private final int partNumber;
        private final int totalParts;

        public TpchBlockIterable(TupleInfo.Type fieldType, int partNumber, int totalParts, String tableName, int fieldIndex)
        {
            checkState(partNumber >= 0, "partNumber must be positive");
            checkState(totalParts > 0, "can not split by 0");

            this.fieldType = fieldType;
            this.partNumber = partNumber;
            this.totalParts = totalParts;
            this.tableName = tableName;
            this.fieldIndex = fieldIndex;
        }

        @Override
        public TupleInfo getTupleInfo()
        {
            return new TupleInfo(fieldType);
        }

        @Override
        public Optional<DataSize> getDataSize()
        {
            return Optional.absent();
        }

        @Override
        public Optional<Integer> getPositionCount()
        {
            return Optional.absent();
        }

        @Override
        public Iterator<Block> iterator()
        {
            return new TpchBlockIterator();
        }

        private class TpchBlockIterator
                extends AbstractIterator<Block>
        {
            private final RecordCursor cursor;

            public TpchBlockIterator()
            {
                this.cursor = data.get(tableName).cursor();

                // Skip the first elements for this iterator.
                for (int i = 0; i < partNumber; i++) {
                    if (!cursor.advanceNextPosition()) {
                        break;
                    }
                }
            }

            @Override
            protected Block computeNext()
            {
                BlockBuilder builder = new BlockBuilder(new TupleInfo(fieldType));

                while (!builder.isFull() && cursor.advanceNextPosition()) {
                    switch (fieldType) {
                        case BOOLEAN:
                            builder.append(cursor.getBoolean(fieldIndex));
                            break;
                        case FIXED_INT_64:
                            builder.append(cursor.getLong(fieldIndex));
                            break;
                        case DOUBLE:
                            builder.append(cursor.getDouble(fieldIndex));
                            break;
                        case VARIABLE_BINARY:
                            builder.append(cursor.getString(fieldIndex));
                            break;
                    }

                    // Split table into multiple pieces. Starts at 1, so
                    // a split of 1 means all the data.
                    for (int i = 1; i < totalParts; i++) {
                        if (!cursor.advanceNextPosition()) {
                            break;
                        }
                    }
                }

                if (builder.isEmpty()) {
                    return endOfData();
                }

                return builder.build();
            }
        }
    }
}
