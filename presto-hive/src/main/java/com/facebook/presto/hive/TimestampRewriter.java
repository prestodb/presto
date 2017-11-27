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
package com.facebook.presto.hive;

import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.AbstractArrayBlock;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.Type;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.function.LongUnaryOperator;

import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.google.common.base.Preconditions.checkArgument;

public class TimestampRewriter
{
    private final List<Type> columnTypes;
    private final DateTimeZone storageTimeZone;

    public TimestampRewriter(List<Type> columnTypes, DateTimeZone storageTimeZone)
    {
        this.columnTypes = columnTypes;
        this.storageTimeZone = storageTimeZone;
    }

    public Page rewritePageHiveToPresto(Page page)
    {
        return modifyTimestampsInPageLazy(page, millis -> millis + storageTimeZone.getOffset(millis));
    }

    public Page rewritePagePrestoToHive(Page page)
    {
        return modifyTimestampsInPage(page, millis -> millis - storageTimeZone.getOffset(millis));
    }

    private Page modifyTimestampsInPageLazy(Page page, LongUnaryOperator modification)
    {
        return modifyTimestampsInPage(page, modification, true);
    }

    private Page modifyTimestampsInPage(Page page, LongUnaryOperator modification)
    {
        return modifyTimestampsInPage(page, modification, false);
    }

    private Page modifyTimestampsInPage(Page page, LongUnaryOperator modification, boolean lazy)
    {
        checkArgument(page.getChannelCount() == columnTypes.size());
        Block[] blocks = new Block[page.getChannelCount()];

        for (int i = 0; i < page.getChannelCount(); ++i) {
            if (lazy) {
                blocks[i] = modifyTimestampsInBlockLazy(page.getBlock(i), columnTypes.get(i), modification);
            }
            else {
                blocks[i] = modifyTimestampsInBlock(page.getBlock(i), columnTypes.get(i), modification);
            }
        }

        return new Page(page.getPositionCount(), blocks);
    }

    private Block modifyTimestampsInBlockLazy(Block block, Type type, LongUnaryOperator modification)
    {
        if (!hasTimestampParameter(type)) {
            return block;
        }
        return new LazyBlock(block.getPositionCount(), lazyBlock -> {
            Block targetBlock = block;
            if (block instanceof LazyBlock) {
                targetBlock = ((LazyBlock) block).getBlock();
            }
            lazyBlock.setBlock(modifyTimestampsInBlock(targetBlock, type, modification));
        });
    }

    private Block modifyTimestampsInBlock(Block block, Type type, LongUnaryOperator modification)
    {
        if (!hasTimestampParameter(type)) {
            return block;
        }

        if (type.equals(TIMESTAMP)) {
            return modifyTimestampsInTimestampBlock(block, modification);
        }
        if (type instanceof ArrayType) {
            return modifyTimestampsInArrayBlock(type, block, modification);
        }
        throw new IllegalArgumentException("Unsupported block; block=" + block.getClass().getName() + "; type=" + type);
    }

    private static boolean hasTimestampParameter(Type type)
    {
        if (type.equals(TIMESTAMP)) {
            return true;
        }
        return type.getTypeParameters().stream().anyMatch(TimestampRewriter::hasTimestampParameter);
    }

    private Block modifyTimestampsInTimestampBlock(Block block, LongUnaryOperator modification)
    {
        BlockBuilder blockBuilder = TIMESTAMP.createFixedSizeBlockBuilder(block.getPositionCount());
        for (int i = 0; i < block.getPositionCount(); ++i) {
            if (block.isNull(i)) {
                blockBuilder.appendNull();
            }
            else {
                long millis = block.getLong(i, 0);
                blockBuilder.writeLong(modification.applyAsLong(millis));
            }
        }

        return blockBuilder.build();
    }

    private Block modifyTimestampsInArrayBlock(Type type, Block block, LongUnaryOperator modification)
    {
        if (block instanceof AbstractArrayBlock) {
            AbstractArrayBlock arrayBlock = (AbstractArrayBlock) block;
            Block innerBlock = modifyTimestampsInBlock(arrayBlock.getValues(),
                    type.getTypeParameters().get(0),
                    modification);

            return new ArrayBlock(arrayBlock.getPositionCount(), arrayBlock.getValueIsNull(), arrayBlock.getOffsets(), innerBlock);
        }
        else {
            // Dear reviewer: This is slow path for sake when ARRAY is not represented as either AbstractArrayBlock or Lazy block wrapping AbstractArrayBlock.
            //                Do we need this? Or should we just throw IllegalArgumentException("unsupported block type") here?
            BlockBuilder builder = type.createBlockBuilder(new BlockBuilderStatus(), block.getPositionCount());
            for (int i = 0; i < block.getPositionCount(); ++i) {
                if (block.isNull(i)) {
                    builder.appendNull();
                }
                else {
                    type.writeObject(builder, modifyTimestampsInBlock(block.getObject(i, Block.class), type.getTypeParameters().get(0), modification));
                }
            }
            return builder.build();
        }
    }
}
