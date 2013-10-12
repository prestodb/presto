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
package com.facebook.presto.serde;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo;

import static com.facebook.presto.block.BlockBuilders.createBlockBuilder;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DictionaryEncoder
        implements Encoder
{
    private final Encoder idWriter;
    private TupleInfo tupleInfo;
    private DictionaryBuilder dictionaryBuilder;
    private boolean finished;

    public DictionaryEncoder(Encoder idWriter)
    {
        this.idWriter = checkNotNull(idWriter, "idWriter is null");
    }

    @Override
    public Encoder append(Block block)
    {
        checkNotNull(block, "tuples is null");
        checkState(!finished, "already finished");

        if (tupleInfo == null) {
            tupleInfo = block.getTupleInfo();
            dictionaryBuilder = new DictionaryBuilder(tupleInfo.getType());
        }

        BlockCursor cursor = block.cursor();
        BlockBuilder idBlockBuilder = createBlockBuilder(TupleInfo.SINGLE_LONG);
        while (cursor.advanceNextPosition()) {
            int key = dictionaryBuilder.putIfAbsent(cursor);
            idBlockBuilder.append(key);
        }
        idWriter.append(idBlockBuilder.build());

        return this;
    }

    @Override
    public BlockEncoding finish()
    {
        checkState(tupleInfo != null, "nothing appended");
        checkState(!finished, "already finished");
        finished = true;

        return new DictionaryBlockEncoding(dictionaryBuilder.build(), idWriter.finish());
    }
}
