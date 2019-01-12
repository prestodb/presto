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
package io.prestosql.operator.scalar;

import com.google.common.collect.ImmutableList;
import io.prestosql.spi.PageBuilder;
import io.prestosql.spi.block.Block;
import io.prestosql.spi.block.BlockBuilder;
import io.prestosql.spi.function.Description;
import io.prestosql.spi.function.ScalarFunction;
import io.prestosql.spi.function.SqlType;
import io.prestosql.spi.function.TypeParameter;
import io.prestosql.spi.type.ArrayType;
import io.prestosql.spi.type.RowType;
import io.prestosql.spi.type.Type;

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
