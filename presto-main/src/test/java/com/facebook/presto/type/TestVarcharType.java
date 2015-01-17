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
package com.facebook.presto.type;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class TestVarcharType
        extends AbstractTestType
{
    public TestVarcharType()
    {
        super(VARCHAR, String.class, createTestBlock());
    }

    public static Block createTestBlock()
    {
        BlockBuilder blockBuilder = VARCHAR.createBlockBuilder(new BlockBuilderStatus());
        VARCHAR.writeString(blockBuilder, "apple");
        VARCHAR.writeString(blockBuilder, "apple");
        VARCHAR.writeString(blockBuilder, "apple");
        VARCHAR.writeString(blockBuilder, "banana");
        VARCHAR.writeString(blockBuilder, "banana");
        VARCHAR.writeString(blockBuilder, "banana");
        VARCHAR.writeString(blockBuilder, "banana");
        VARCHAR.writeString(blockBuilder, "banana");
        VARCHAR.writeString(blockBuilder, "cherry");
        VARCHAR.writeString(blockBuilder, "cherry");
        VARCHAR.writeString(blockBuilder, "date");
        return blockBuilder.build();
    }

    @Override
    protected Object getGreaterValue(Object value)
    {
        return Slices.utf8Slice(((Slice) value).toStringUtf8() + "_");
    }
}
