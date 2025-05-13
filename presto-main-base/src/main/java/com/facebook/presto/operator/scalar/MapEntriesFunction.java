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

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;

import static com.google.common.base.Verify.verify;

@ScalarFunction("map_entries")
@Description("construct an array of entries from a given map")
public class MapEntriesFunction
{
    @TypeParameter("K")
    @TypeParameter("V")
    public MapEntriesFunction(@TypeParameter("array(row(K,V))") Type arrayType) {}

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
        int entryCount = block.getPositionCount() / 2;
        BlockBuilder blockBuilder = arrayType.createBlockBuilder(null, entryCount);
        BlockBuilder entryBuilder = blockBuilder.beginBlockEntry();
        for (int i = 0; i < entryCount; i++) {
            BlockBuilder rowBuilder = entryBuilder.beginBlockEntry();
            keyType.appendTo(block, 2 * i, rowBuilder);
            valueType.appendTo(block, 2 * i + 1, rowBuilder);
            entryBuilder.closeEntry();
        }

        blockBuilder.closeEntry();
        return arrayType.getObject(blockBuilder, blockBuilder.getPositionCount() - 1);
    }
}
