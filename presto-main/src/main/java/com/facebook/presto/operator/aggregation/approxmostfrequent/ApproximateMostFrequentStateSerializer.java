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
package com.facebook.presto.operator.aggregation.approxmostfrequent;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.approxmostfrequent.stream.StreamSummary;
import com.facebook.presto.spi.function.AccumulatorStateSerializer;
import com.facebook.presto.spi.function.TypeParameter;
import com.google.common.collect.ImmutableList;

public class ApproximateMostFrequentStateSerializer
        implements AccumulatorStateSerializer<ApproximateMostFrequentState>
{
    private final Type type;
    private final Type serializedType;
    private static final String MAX_BUCKETS = "max_buckets";
    private static final String CAPACITY = "capacity";
    private static final String KEYS = "keys";
    private static final String VALUES = "values";

    public ApproximateMostFrequentStateSerializer(@TypeParameter("T") Type type)
    {
        this.type = type;
        this.serializedType = RowType.from(ImmutableList.of(RowType.field(MAX_BUCKETS, BigintType.BIGINT),
                RowType.field(CAPACITY, BigintType.BIGINT),
                RowType.field(KEYS, new ArrayType(type)),
                RowType.field(VALUES, new ArrayType(BigintType.BIGINT))));
    }

    @Override
    public Type getSerializedType()
    {
        return serializedType;
    }

    @Override
    public void serialize(ApproximateMostFrequentState state, BlockBuilder out)
    {
        if (state.getStateSummary() == null) {
            out.appendNull();
        }
        else {
            state.getStateSummary().serialize(out);
        }
    }

    @Override
    public void deserialize(Block block, int index, ApproximateMostFrequentState state)
    {
        Block currentBlock = (Block) serializedType.getObject(block, index);
        state.setStateSummary(StreamSummary.deserialize(type, currentBlock));
    }
}
