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

import com.facebook.presto.operator.aggregation.TypedSet;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.spi.function.TypeParameter;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.Verify.verify;

@ScalarFunction("multimap_from_entries")
@Description("construct a multimap from an array of entries")
public final class MultimapFromEntriesFunction
{
    private static final String NAME = "multimap_from_entries";
    private static final int INITIAL_ENTRY_COUNT = 128;

    private final PageBuilder pageBuilder;
    private IntList[] entryIndicesList;

    @TypeParameter("K")
    @TypeParameter("V")
    public MultimapFromEntriesFunction(@TypeParameter("map(K,array(V))") Type mapType)
    {
        pageBuilder = new PageBuilder(ImmutableList.of(mapType));
        initializeEntryIndicesList(INITIAL_ENTRY_COUNT);
    }

    @TypeParameter("K")
    @TypeParameter("V")
    @SqlType("map(K,array(V))")
    @SqlNullable
    public Block multimapFromEntries(
            @TypeParameter("map(K,array(V))") MapType mapType,
            @SqlType("array(row(K,V))") Block block)
    {
        Type keyType = mapType.getKeyType();
        Type valueType = ((ArrayType) mapType.getValueType()).getElementType();
        RowType rowType = RowType.anonymous(ImmutableList.of(keyType, valueType));

        if (pageBuilder.isFull()) {
            pageBuilder.reset();
        }

        int entryCount = block.getPositionCount();
        if (entryCount > entryIndicesList.length) {
            initializeEntryIndicesList(entryCount);
        }
        TypedSet keySet = new TypedSet(keyType, entryCount, NAME);

        for (int i = 0; i < entryCount; i++) {
            if (block.isNull(i)) {
                clearEntryIndices(keySet.size());
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "map entry cannot be null");
            }
            Block rowBlock = rowType.getObject(block, i);

            if (rowBlock.isNull(0)) {
                clearEntryIndices(keySet.size());
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "map key cannot be null");
            }

            if (keySet.contains(rowBlock, 0)) {
                entryIndicesList[keySet.positionOf(rowBlock, 0)].add(i);
            }
            else {
                keySet.add(rowBlock, 0);
                entryIndicesList[keySet.size() - 1].add(i);
            }
        }

        BlockBuilder multimapBlockBuilder = pageBuilder.getBlockBuilder(0);
        BlockBuilder singleMapWriter = multimapBlockBuilder.beginBlockEntry();
        for (int i = 0; i < keySet.size(); i++) {
            keyType.appendTo(rowType.getObject(block, entryIndicesList[i].getInt(0)), 0, singleMapWriter);
            BlockBuilder singleArrayWriter = singleMapWriter.beginBlockEntry();
            for (int entryIndex : entryIndicesList[i]) {
                valueType.appendTo(rowType.getObject(block, entryIndex), 1, singleArrayWriter);
            }
            singleMapWriter.closeEntry();
        }

        multimapBlockBuilder.closeEntry();
        pageBuilder.declarePosition();
        clearEntryIndices(keySet.size());
        return mapType.getObject(multimapBlockBuilder, multimapBlockBuilder.getPositionCount() - 1);
    }

    private void clearEntryIndices(int entryCount)
    {
        verify(entryCount <= entryIndicesList.length);
        for (int i = 0; i < entryCount; i++) {
            entryIndicesList[i].clear();
        }
    }

    private void initializeEntryIndicesList(int entryCount)
    {
        entryIndicesList = new IntList[entryCount];
        for (int i = 0; i < entryIndicesList.length; i++) {
            entryIndicesList[i] = new IntArrayList();
        }
    }
}
