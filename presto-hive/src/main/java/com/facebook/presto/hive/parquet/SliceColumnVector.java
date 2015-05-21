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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.type.Type;
import io.airlift.slice.Slices;
import parquet.column.page.DataPage;

import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class SliceColumnVector extends ColumnVector
{
    public SliceColumnVector(Type type)
    {
        super(type);
    }

    @Override
    public Block getBlock()
    {
        BlockBuilder blockBuilder = type.createBlockBuilder(new BlockBuilderStatus(), numValues);
        if (pages != null) {
            for (int i = 0; i < pages.length; i++) {
                DataPage page = pages[i];
                for (int j = 0; j < page.getValueCount(); j++) {
                    VARCHAR.writeSlice(blockBuilder, Slices.wrappedBuffer(readers[i].readBytes().getBytes()));
                }
            }
        }
        return blockBuilder.build();
    }
}
