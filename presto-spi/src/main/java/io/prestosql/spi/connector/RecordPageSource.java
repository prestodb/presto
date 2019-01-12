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
package io.prestosql.spi.connector;

import io.airlift.slice.Slice;
import io.prestosql.spi.Page;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.type.Type;

import java.util.ArrayList;
import java.util.List;

import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public class RecordPageSource
        implements ConnectorPageSource
{
    private static final int ROWS_PER_REQUEST = 4096;
    private final RecordCursor cursor;
    private final List<Type> types;
    private final PageBuilder pageBuilder;
    private boolean closed;

    public RecordPageSource(RecordSet recordSet)
    {
        this(requireNonNull(recordSet, "recordSet is null").getColumnTypes(), recordSet.cursor());
    }

    public RecordPageSource(List<Type> types, RecordCursor cursor)
    {
        this.cursor = requireNonNull(cursor, "cursor is null");
        this.types = unmodifiableList(new ArrayList<>(requireNonNull(types, "types is null")));
        this.pageBuilder = new PageBuilder(this.types);
    }

    public RecordCursor getCursor()
    {
        return cursor;
    }

    @Override
    public long getCompletedBytes()
    {
        return cursor.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return cursor.getReadTimeNanos();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return cursor.getSystemMemoryUsage() + pageBuilder.getSizeInBytes();
    }

    @Override
    public void close()
    {
        closed = true;
        cursor.close();
    }

    @Override
    public boolean isFinished()
    {
        return closed && pageBuilder.isEmpty();
    }

    @Override
    public Page getNextPage()
    {
        if (!closed) {
            int i;
            for (i = 0; i < ROWS_PER_REQUEST; i++) {
                if (pageBuilder.isFull()) {
                    break;
                }

                if (!cursor.advanceNextPosition()) {
                    closed = true;
                    break;
                }

                pageBuilder.declarePosition();
                for (int column = 0; column < types.size(); column++) {
                    BlockBuilder output = pageBuilder.getBlockBuilder(column);
                    if (cursor.isNull(column)) {
                        output.appendNull();
                    }
                    else {
                        Type type = types.get(column);
                        Class<?> javaType = type.getJavaType();
                        if (javaType == boolean.class) {
                            type.writeBoolean(output, cursor.getBoolean(column));
                        }
                        else if (javaType == long.class) {
                            type.writeLong(output, cursor.getLong(column));
                        }
                        else if (javaType == double.class) {
                            type.writeDouble(output, cursor.getDouble(column));
                        }
                        else if (javaType == Slice.class) {
                            Slice slice = cursor.getSlice(column);
                            type.writeSlice(output, slice, 0, slice.length());
                        }
                        else {
                            type.writeObject(output, cursor.getObject(column));
                        }
                    }
                }
            }
        }

        // only return a page if the buffer is full or we are finishing
        if (pageBuilder.isEmpty() || (!closed && !pageBuilder.isFull())) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();

        return page;
    }
}
