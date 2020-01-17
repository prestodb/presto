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
package com.facebook.presto.operator.scalar;

import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import static com.google.common.base.Verify.verify;

@ScalarFunction("map_entries")
@Description("construct an array of entries from a given map")
public class MapEntriesFunction
{
    private final PageBuilder pageBuilder;

    @TypeParameter("K")
    @TypeParameter("V")
    public MapEntriesFunction(@TypeParameter("array(row(K,V))") Type arrayType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(arrayType));
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType("array(row(K,V))")
    public Block mapFromEntries(
            @TypeParameter("row(K,V)") RowType rowType,
            @SqlType("map(K,V)") Block block)
    {
        verify(rowType.getTypeParameters().size() == 2);
        verify(block.getPositionCount() % 2 == 0);

        Type keyType = rowType.getTypeParameters().get(0);
        Type valueType = rowType.getTypeParameters().get(1);
        ArrayType arrayType = new ArrayType(rowType);

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        int entryCount = block.getPositionCount() / 2;
        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
        for (int i = 0; i < entryCount; i++) {
            BlockBuilder rowBuilder = entryBuilder.beginBlockEntry();
            keyType.appendTo(block, 2 * i, rowBuilder);
            valueType.appendTo(block, 2 * i + 1, rowBuilder);
            entryBuilder.closeEntry();
        }

        blockBuilder.closeEntry();
        pageBuilder.declarePosition();
        return arrayType.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
    }
}
