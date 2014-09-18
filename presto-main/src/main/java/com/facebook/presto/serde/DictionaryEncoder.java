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

import com.facebook.presto.block.dictionary.DictionaryBlockEncoding;
import com.facebook.presto.operator.GroupByHash;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.BlockEncoding;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

public class DictionaryEncoder
        implements Encoder
{
    private final Type type;
    private final Encoder idWriter;
    private final GroupByHash dictionaryBuilder;
    private boolean finished;

    public DictionaryEncoder(Type type, Encoder idWriter)
    {
        this.type = type;
        this.idWriter = checkNotNull(idWriter, "idWriter is null");
        this.dictionaryBuilder = new GroupByHash(ImmutableList.of(type), new int[] {0}, 1_000);
    }

    @Override
    public Encoder append(Block block)
    {
        checkNotNull(block, "block is null");
        checkState(!finished, "already finished");

        BlockBuilder idBlockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus());
        for (int position = 0; position < block.getPositionCount(); position++) {
            int key = dictionaryBuilder.putIfAbsent(position, block);
            BIGINT.writeLong(idBlockBuilder, key);
        }
        idWriter.append(idBlockBuilder.build());

        return this;
    }

    @Override
    public BlockEncoding finish()
    {
        checkState(!finished, "already finished");
        finished = true;

        PageBuilder pageBuilder = new PageBuilder(ImmutableList.of(type));
        for (int groupId = 0; groupId < dictionaryBuilder.getGroupCount(); groupId++) {
            dictionaryBuilder.appendValuesTo(groupId, pageBuilder, 0);
        }
        return new DictionaryBlockEncoding(pageBuilder.build().getBlock(0), idWriter.finish());
    }
}
