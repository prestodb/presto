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

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockCursor;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.block.dictionary.DictionaryBlockEncoding;
import com.facebook.presto.spi.type.Type;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DictionaryEncoder
        implements Encoder
{
    private final Encoder idWriter;
    private Type type;
    private DictionaryBuilder dictionaryBuilder;
    private boolean finished;

    public DictionaryEncoder(Encoder idWriter)
    {
        this.idWriter = checkNotNull(idWriter, "idWriter is null");
    }

    @Override
    public Encoder append(Block block)
    {
        checkNotNull(block, "block is null");
        checkState(!finished, "already finished");

        if (type == null) {
            type = block.getType();
            dictionaryBuilder = new DictionaryBuilder(type);
        }

        BlockCursor cursor = block.cursor();
        BlockBuilder idBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus());
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
        checkState(type != null, "nothing appended");
        checkState(!finished, "already finished");
        finished = true;

        return new DictionaryBlockEncoding(dictionaryBuilder.build(), idWriter.finish());
    }
}
