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
package com.facebook.presto.iceberg.function.changelog;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.TypeParameter;
import com.google.common.collect.ImmutableList;

public final class ApplyChangelogStateSerializer
        implements AccumulatorStateSerializer<ApplyChangelogState>
{
    private final Type serializedType;

    public ApplyChangelogStateSerializer(@TypeParameter("T") Type innerType)
    {
        this.serializedType = getSerializedRowType(innerType);
    }

    private static Type getSerializedRowType(Type inner)
    {
        return RowType.anonymous(ImmutableList.of(BigintType.BIGINT, VarcharType.VARCHAR, inner));
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(ApplyChangelogState state, BlockBuilder out)
    {
        if (!state.getRecord().isPresent()) {
            out.appendNull();
        }
        else {
            state.getRecord().get().serialize(out);
        }
    }

    @Override
    public void deserialize(Block block, int index, ApplyChangelogState state)
    {
        ChangelogRecord record = new ChangelogRecord(state.getType());
        record.deserialize(block, index);
        state.setRecord(record);
    }
}
