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
package com.facebook.presto.spi;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slice;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class RecordPageSink
        implements ConnectorPageSink
{
    private final RecordSink recordSink;

    public RecordPageSink(RecordSink recordSink)
    {
        this.recordSink = requireNonNull(recordSink, "recordSink is null");
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        return completedFuture(recordSink.commit());
    }

    @Override
    public void abort()
    {
        recordSink.rollback();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        Block[] blocks = page.getBlocks();
        List<Type> columnTypes = recordSink.getColumnTypes();

        for (int position = 0; position < page.getPositionCount(); position++) {
            recordSink.beginRecord();
            for (int i = 0; i < blocks.length; i++) {
                writeField(position, blocks[i], columnTypes.get(i));
            }
            recordSink.finishRecord();
        }
        return NOT_BLOCKED;
    }

    private void writeField(int position, Block block, Type type)
    {
        if (block.isNull(position)) {
            recordSink.appendNull();
            return;
        }
        if (type.getJavaType() == boolean.class) {
            recordSink.appendBoolean(type.getBoolean(block, position));
        }
        else if (type.getJavaType() == long.class) {
            recordSink.appendLong(type.getLong(block, position));
        }
        else if (type.getJavaType() == double.class) {
            recordSink.appendDouble(type.getDouble(block, position));
        }
        else if (type.getJavaType() == Slice.class) {
            recordSink.appendString(type.getSlice(block, position).getBytes());
        }
        else {
            recordSink.appendObject(type.getObject(block, position));
        }
    }
}
