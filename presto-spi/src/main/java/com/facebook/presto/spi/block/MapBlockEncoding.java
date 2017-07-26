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

package com.facebook.presto.spi.block;

import com.facebook.presto.spi.function.OperatorType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSerde;
import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;

import java.lang.invoke.MethodHandle;

import static com.facebook.presto.spi.block.AbstractMapBlock.HASH_MULTIPLIER;
import static com.facebook.presto.spi.block.MethodHandleUtil.compose;
import static com.facebook.presto.spi.block.MethodHandleUtil.nativeValueGetter;
import static io.airlift.slice.Slices.wrappedIntArray;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class MapBlockEncoding
        implements BlockEncoding
{
    public static final BlockEncodingFactory<MapBlockEncoding> FACTORY = new MapBlockEncodingFactory();
    private static final String NAME = "MAP";

    private final Type keyType;
    private final MethodHandle keyNativeHashCode;
    private final MethodHandle keyBlockNativeEquals;
    private final BlockEncoding keyBlockEncoding;
    private final BlockEncoding valueBlockEncoding;

    public MapBlockEncoding(Type keyType, MethodHandle keyBlockNativeEquals, MethodHandle keyNativeHashCode, BlockEncoding keyBlockEncoding, BlockEncoding valueBlockEncoding)
    {
        this.keyType = requireNonNull(keyType, "keyType is null");
        // keyNativeHashCode can only be null due to map block kill switch. deprecated.new-map-block
        this.keyNativeHashCode = keyNativeHashCode;
        // keyBlockNativeEquals can only be null due to map block kill switch. deprecated.new-map-block
        this.keyBlockNativeEquals = keyBlockNativeEquals;
        this.keyBlockEncoding = requireNonNull(keyBlockEncoding, "keyBlockEncoding is null");
        this.valueBlockEncoding = requireNonNull(valueBlockEncoding, "valueBlockEncoding is null");
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(SliceOutput sliceOutput, Block block)
    {
        AbstractMapBlock mapBlock = (AbstractMapBlock) block;

        int positionCount = mapBlock.getPositionCount();

        int offsetBase = mapBlock.getOffsetBase();
        int[] offsets = mapBlock.getOffsets();
        int[] hashTable = mapBlock.getHashTables();

        int entriesStartOffset = offsets[offsetBase];
        int entriesEndOffset = offsets[offsetBase + positionCount];
        keyBlockEncoding.writeBlock(sliceOutput, mapBlock.getKeys().getRegion(entriesStartOffset, entriesEndOffset - entriesStartOffset));
        valueBlockEncoding.writeBlock(sliceOutput, mapBlock.getValues().getRegion(entriesStartOffset, entriesEndOffset - entriesStartOffset));

        sliceOutput.appendInt((entriesEndOffset - entriesStartOffset) * HASH_MULTIPLIER);
        sliceOutput.writeBytes(wrappedIntArray(hashTable, entriesStartOffset * HASH_MULTIPLIER, (entriesEndOffset - entriesStartOffset) * HASH_MULTIPLIER));

        sliceOutput.appendInt(positionCount);
        for (int position = 0; position < positionCount + 1; position++) {
            sliceOutput.writeInt(offsets[offsetBase + position] - entriesStartOffset);
        }
        EncoderUtil.encodeNullsAsBits(sliceOutput, block);
    }

    @Override
    public Block readBlock(SliceInput sliceInput)
    {
        Block keyBlock = keyBlockEncoding.readBlock(sliceInput);
        Block valueBlock = valueBlockEncoding.readBlock(sliceInput);

        int[] hashTable = new int[sliceInput.readInt()];
        sliceInput.readBytes(wrappedIntArray(hashTable));

        if (keyBlock.getPositionCount() != valueBlock.getPositionCount() || keyBlock.getPositionCount() * HASH_MULTIPLIER != hashTable.length) {
            throw new IllegalArgumentException(
                    format("Deserialized MapBlock violates invariants: key %d, value %d, hash %d", keyBlock.getPositionCount(), valueBlock.getPositionCount(), hashTable.length));
        }

        int positionCount = sliceInput.readInt();
        int[] offsets = new int[positionCount + 1];
        sliceInput.readBytes(wrappedIntArray(offsets));
        boolean[] mapIsNull = EncoderUtil.decodeNullBits(sliceInput, positionCount);
        return new MapBlock(0, positionCount, mapIsNull, offsets, keyBlock, valueBlock, hashTable, keyType, keyBlockNativeEquals, keyNativeHashCode);
    }

    @Override
    public BlockEncodingFactory getFactory()
    {
        return FACTORY;
    }

    public static class MapBlockEncodingFactory
            implements BlockEncodingFactory<MapBlockEncoding>
    {
        @Override
        public String getName()
        {
            return NAME;
        }

        @Override
        public MapBlockEncoding readEncoding(TypeManager typeManager, BlockEncodingSerde serde, SliceInput input)
        {
            Type keyType = TypeSerde.readType(typeManager, input);
            MethodHandle keyNativeEquals = typeManager.resolveOperator(OperatorType.EQUAL, asList(keyType, keyType));
            MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
            MethodHandle keyNativeHashCode = typeManager.resolveOperator(OperatorType.HASH_CODE, singletonList(keyType));

            BlockEncoding keyBlockEncoding = serde.readBlockEncoding(input);
            BlockEncoding valueBlockEncoding = serde.readBlockEncoding(input);
            return new MapBlockEncoding(keyType, keyBlockNativeEquals, keyNativeHashCode, keyBlockEncoding, valueBlockEncoding);
        }

        @Override
        public void writeEncoding(BlockEncodingSerde serde, SliceOutput output, MapBlockEncoding blockEncoding)
        {
            TypeSerde.writeType(output, blockEncoding.keyType);
            serde.writeBlockEncoding(output, blockEncoding.keyBlockEncoding);
            serde.writeBlockEncoding(output, blockEncoding.valueBlockEncoding);
        }
    }
}
