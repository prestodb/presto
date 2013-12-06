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

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.tuple.TupleInfo;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class RowPageBuilder
{
    public static RowPageBuilder rowPageBuilder(TupleInfo... tupleInfos)
    {
        return rowPageBuilder(ImmutableList.copyOf(tupleInfos));
    }

    public static RowPageBuilder rowPageBuilder(Iterable<TupleInfo> tupleInfos)
    {
        return new RowPageBuilder(tupleInfos);
    }

    private final List<BlockBuilder> builders;
    private long rowCount;

    RowPageBuilder(Iterable<TupleInfo> tupleInfos)
    {
        checkNotNull(tupleInfos, "tupleInfos is null");
        ImmutableList.Builder<BlockBuilder> builders = ImmutableList.builder();
        for (TupleInfo tupleInfo : tupleInfos) {
            builders.add(new BlockBuilder(tupleInfo));
        }
        this.builders = builders.build();
        checkArgument(!this.builders.isEmpty(), "At least one tuple info is required");
    }

    public boolean isEmpty()
    {
        return rowCount == 0;
    }

    public RowPageBuilder row(Object... values)
    {
        checkArgument(values.length == builders.size(), "Expected %s values, but got %s", builders.size(), values.length);

        for (int channel = 0; channel < values.length; channel++) {
            append(builders.get(channel), values[channel]);
        }
        rowCount++;
        return this;
    }

    public Page build()
    {
        Block[] blocks = new Block[builders.size()];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = builders.get(i).build();
        }
        return new Page(blocks);
    }

    private void append(BlockBuilder builder, Object value)
    {
        if (value == null) {
            builder.appendNull();
        }
        else if (value instanceof Boolean) {
            builder.append((Boolean) value);
        }
        else if ((value instanceof Long) || (value instanceof Integer)) {
            builder.append(((Number) value).longValue());
        }
        else if (value instanceof Double) {
            builder.append((Double) value);
        }
        else if (value instanceof String) {
            builder.append((String) value);
        }
        else {
            throw new IllegalArgumentException("bad value: " + value.getClass().getName());
        }
    }
}
