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
package com.facebook.presto.block;

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.type.ArrayType;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slices;
import org.testng.annotations.Test;

import java.util.List;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertNotEquals;
import static org.testng.Assert.assertTrue;

public class TestBlockBuilder
{
    @Test
    public void testMultipleValuesWithNull()
    {
        BlockBuilder blockBuilder = BIGINT.createBlockBuilder(new BlockBuilderStatus(), 10);
        blockBuilder.appendNull();
        BIGINT.writeLong(blockBuilder, 42);
        blockBuilder.appendNull();
        BIGINT.writeLong(blockBuilder, 42);
        Block block = blockBuilder.build();

        assertTrue(block.isNull(0));
        assertEquals(BIGINT.getLong(block, 1), 42L);
        assertTrue(block.isNull(2));
        assertEquals(BIGINT.getLong(block, 3), 42L);
    }

    @Test
    public void testNewBlockBuilderLike()
    {
        ArrayType longArrayType = new ArrayType(BIGINT);
        ArrayType arrayType = new ArrayType(longArrayType);
        List<Type> channels = ImmutableList.of(BIGINT, VARCHAR, arrayType);
        PageBuilder pageBuilder = new PageBuilder(channels);
        BlockBuilder bigintBlockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder varcharBlockBuilder = pageBuilder.getBlockBuilder(1);
        BlockBuilder arrayBlockBuilder = pageBuilder.getBlockBuilder(2);

        for (int i = 0; i < 100; i++) {
            BIGINT.writeLong(bigintBlockBuilder, i);
            VARCHAR.writeSlice(varcharBlockBuilder, Slices.utf8Slice("test" + i));
            Block longArrayBlock = new ArrayType(BIGINT)
                    .createBlockBuilder(new BlockBuilderStatus(), 1)
                    .writeObject(BIGINT.createBlockBuilder(new BlockBuilderStatus(), 2).writeLong(i).closeEntry().writeLong(i * 2).closeEntry().build())
                    .closeEntry();
            arrayBlockBuilder.writeObject(longArrayBlock).closeEntry();
            pageBuilder.declarePosition();
        }

        PageBuilder newPageBuilder = pageBuilder.newPageBuilderLike();
        for (int i = 0; i < channels.size(); i++) {
            assertEquals(newPageBuilder.getType(i), pageBuilder.getType(i));
            // we should get new block builder instances
            assertNotEquals(pageBuilder.getBlockBuilder(i), newPageBuilder.getBlockBuilder(i));
            assertEquals(newPageBuilder.getBlockBuilder(i).getPositionCount(), 0);
            assertTrue(newPageBuilder.getBlockBuilder(i).getRetainedSizeInBytes() < pageBuilder.getBlockBuilder(i).getRetainedSizeInBytes());
        }
    }
}
