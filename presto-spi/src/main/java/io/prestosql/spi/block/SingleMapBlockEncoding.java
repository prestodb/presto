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

package io.prestosql.spi.block;

import io.airlift.slice.SliceInput;
import io.airlift.slice.SliceOutput;
import io.prestosql.spi.function.OperatorType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.spi.type.TypeSerde;

import java.lang.invoke.MethodHandle;

import static io.airlift.slice.Slices.wrappedIntArray;
import static io.prestosql.spi.block.AbstractMapBlock.HASH_MULTIPLIER;
import static io.prestosql.spi.block.MethodHandleUtil.compose;
import static io.prestosql.spi.block.MethodHandleUtil.nativeValueGetter;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

public class SingleMapBlockEncoding
        implements BlockEncoding
{
    public static final String NAME = "MAP_ELEMENT";

    private final TypeManager typeManager;

    public SingleMapBlockEncoding(TypeManager typeManager)
    {
        this.typeManager = requireNonNull(typeManager, "typeManager is null");
    }

    @Override
    public String getName()
    {
        return NAME;
    }

    @Override
    public void writeBlock(BlockEncodingSerde blockEncodingSerde, SliceOutput sliceOutput, Block block)
    {
        SingleMapBlock singleMapBlock = (SingleMapBlock) block;
        TypeSerde.writeType(sliceOutput, singleMapBlock.keyType);

        int offset = singleMapBlock.getOffset();
        int positionCount = singleMapBlock.getPositionCount();
        blockEncodingSerde.writeBlock(sliceOutput, singleMapBlock.getRawKeyBlock().getRegion(offset / 2, positionCount / 2));
        blockEncodingSerde.writeBlock(sliceOutput, singleMapBlock.getRawValueBlock().getRegion(offset / 2, positionCount / 2));
        int[] hashTable = singleMapBlock.getHashTable();
        sliceOutput.appendInt(positionCount / 2 * HASH_MULTIPLIER);
        sliceOutput.writeBytes(wrappedIntArray(hashTable, offset / 2 * HASH_MULTIPLIER, positionCount / 2 * HASH_MULTIPLIER));
    }

    @Override
    public Block readBlock(BlockEncodingSerde blockEncodingSerde, SliceInput sliceInput)
    {
        Type keyType = TypeSerde.readType(typeManager, sliceInput);
        MethodHandle keyNativeEquals = typeManager.resolveOperator(OperatorType.EQUAL, asList(keyType, keyType));
        MethodHandle keyBlockNativeEquals = compose(keyNativeEquals, nativeValueGetter(keyType));
        MethodHandle keyNativeHashCode = typeManager.resolveOperator(OperatorType.HASH_CODE, singletonList(keyType));

        Block keyBlock = blockEncodingSerde.readBlock(sliceInput);
        Block valueBlock = blockEncodingSerde.readBlock(sliceInput);

        int[] hashTable = new int[sliceInput.readInt()];
        sliceInput.readBytes(wrappedIntArray(hashTable));

        if (keyBlock.getPositionCount() != valueBlock.getPositionCount() || keyBlock.getPositionCount() * HASH_MULTIPLIER != hashTable.length) {
            throw new IllegalArgumentException(
                    format("Deserialized SingleMapBlock violates invariants: key %d, value %d, hash %d", keyBlock.getPositionCount(), valueBlock.getPositionCount(), hashTable.length));
        }

        return new SingleMapBlock(0, keyBlock.getPositionCount() * 2, keyBlock, valueBlock, hashTable, keyType, keyNativeHashCode, keyBlockNativeEquals);
    }
}
