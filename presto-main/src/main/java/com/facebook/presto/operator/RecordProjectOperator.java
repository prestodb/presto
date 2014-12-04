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
package com.facebook.presto.operator;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.io.Closeable;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class RecordProjectOperator
        implements Operator, Closeable
{
    private static final int ROWS_PER_REQUEST = 4096;
    private final OperatorContext operatorContext;
    private final RecordCursor cursor;
    private final List<Type> types;
    private final PageBuilder pageBuilder;
    private boolean finishing;
    private long completedBytes;
    private long readTimeNanos;

    public RecordProjectOperator(OperatorContext operatorContext, RecordSet recordSet)
    {
        this(operatorContext, recordSet.getColumnTypes(), recordSet.cursor());
    }

    public RecordProjectOperator(OperatorContext operatorContext, List<Type> columnTypes, RecordCursor cursor)
    {
        this.operatorContext = checkNotNull(operatorContext, "operatorContext is null");
        this.cursor = checkNotNull(cursor, "cursor is null");

        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (Type columnType : columnTypes) {
            types.add(columnType);
        }
        this.types = types.build();

        pageBuilder = new PageBuilder(getTypes());
    }

    @Override
    public OperatorContext getOperatorContext()
    {
        return operatorContext;
    }

    public RecordCursor getCursor()
    {
        return cursor;
    }

    @Override
    public List<Type> getTypes()
    {
        return types;
    }

    @Override
    public void finish()
    {
        close();
    }

    @Override
    public void close()
    {
        finishing = true;
        cursor.close();
    }

    @Override
    public boolean isFinished()
    {
        return finishing && pageBuilder.isEmpty();
    }

    @Override
    public boolean needsInput()
    {
        return false;
    }

    @Override
    public void addInput(Page page)
    {
        throw new UnsupportedOperationException(getClass().getName() + " can not take input");
    }

    @Override
    public Page getOutput()
    {
        if (!finishing) {
            int i;
            for (i = 0; i < ROWS_PER_REQUEST; i++) {
                if (pageBuilder.isFull()) {
                    break;
                }

                if (!cursor.advanceNextPosition()) {
                    finishing = true;
                    break;
                }

                pageBuilder.declarePosition();
                for (int column = 0; column < types.size(); column++) {
                    BlockBuilder output = pageBuilder.getBlockBuilder(column);
                    if (cursor.isNull(column)) {
                        output.appendNull();
                    }
                    else {
                        Type type = getTypes().get(column);
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
                            throw new AssertionError("Unimplemented type: " + javaType.getName());
                        }
                    }
                }
            }

            long endCompletedBytes = cursor.getCompletedBytes();
            long endReadTimeNanos = cursor.getReadTimeNanos();
            operatorContext.recordGeneratedInput(endCompletedBytes - completedBytes, i, endReadTimeNanos - readTimeNanos);
            completedBytes = endCompletedBytes;
            readTimeNanos = endReadTimeNanos;
        }

        // only return a full page is buffer is full or we are finishing
        if (pageBuilder.isEmpty() || (!finishing && !pageBuilder.isFull())) {
            return null;
        }

        Page page = pageBuilder.build();
        pageBuilder.reset();

        return page;
    }
}
