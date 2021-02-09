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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.Type;
import org.openjdk.jol.info.ClassLayout;

import static com.facebook.presto.common.type.TypeUtils.isExactNumericType;
import static com.facebook.presto.type.TypeUtils.expectedValueSize;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;

public abstract class MapUnionSumResult
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(MapUnionSumResult.class).instanceSize();
    private static final int EXPECTED_ENTRIES = 10;
    private static final int EXPECTED_ENTRY_SIZE = 16;

    protected final Type keyType;
    protected final Type valueType;
    protected final Adder adder;

    public MapUnionSumResult(Type keyType, Type valueType, Adder adder)
    {
        this.keyType = requireNonNull(keyType, "keyType is null");
        this.valueType = requireNonNull(valueType, "valueType is null");
        this.adder = adder;
    }

    abstract int size();
    abstract void addKey(int i, BlockBuilder out);
    abstract void appendValue(int i, BlockBuilder blockBuilder);
    abstract boolean isValueNull(int i);
    public abstract long getRetainedSizeInBytes();
    abstract void addKeyToSet(TypedSet keySet, int i);
    abstract int getPosition(TypedSet otherKeySet, int keyPosition);
    abstract TypedSet getKeySet();
    abstract Block getValueBlock();
    abstract int getValueBlockIndex(int i);

    public static MapUnionSumResult create(Type keyType, Type valueType, Adder adder, Block mapBlock)
    {
        return new SingleMapBlock(keyType, valueType, adder, mapBlock);
    }

    public Type getKeyType()
    {
        return keyType;
    }

    public void serialize(BlockBuilder out)
    {
        BlockBuilder mapBlockBuilder = out.beginBlockEntry();
        for (int i = 0; i < size(); i++) {
            addKey(i, mapBlockBuilder);
            appendValue(i, mapBlockBuilder);
        }
        out.closeEntry();
    }

    static void appendValue(Type valueType, Block block, int position, BlockBuilder blockBuilder)
    {
        if (block.isNull(position)) {
            if (isExactNumericType(valueType)) {
                valueType.writeLong(blockBuilder, 0L);
            }
            else {
                valueType.writeDouble(blockBuilder, 0);
            }
        }
        else {
            valueType.appendTo(block, position, blockBuilder);
        }
    }

    /**
     * Add the values for corresponding keys from other to this.
     */
    public MapUnionSumResult unionSum(MapUnionSumResult other)
    {
        if (other instanceof KeySetAndValues &&
                (this instanceof SingleMapBlock || size() < other.size())) {
            // We can avoid creating a new set by merging the keys from this into the other one.
            return other.unionSum(this);
        }

        TypedSet resultKeySet = getKeySet();

        int size = size();
        int otherSize = other.size();
        int[] otherKeyIndex = new int[size];
        boolean[] toAppend = new boolean[otherSize];
        boolean[] common = new boolean[size];
        boolean remaining = false;
        BlockBuilder resultValueBlockBuilder = valueType.createBlockBuilder(null, max(size(), other.size()), expectedValueSize(valueType, EXPECTED_ENTRY_SIZE));
        for (int i = 0; i < otherSize; i++) {
            int position = other.getPosition(resultKeySet, i);
            if (position >= 0) {
                otherKeyIndex[position] = i;
                common[position] = true;
            }
            else {
                toAppend[i] = true;
                if (!remaining) {
                    remaining = true;
                }
            }
        }

        for (int i = 0; i < size; i++) {
            if (common[i]) {
                if (!isValueNull(i) && !other.isValueNull(otherKeyIndex[i])) {
                    // common key. So SUM the values from both.
                    adder.writeSum(
                            valueType,
                            getValueBlock(),
                            getValueBlockIndex(i),
                            other.getValueBlock(),
                            other.getValueBlockIndex(otherKeyIndex[i]),
                            resultValueBlockBuilder);
                }
                else if (!isValueNull(i)) {
                    this.appendValue(i, resultValueBlockBuilder);
                }
                else {
                    other.appendValue(otherKeyIndex[i], resultValueBlockBuilder);
                }
            }
            else {
                this.appendValue(i, resultValueBlockBuilder);
            }
        }

        // Now the remaining entries from other that are not in the current keyset
        if (remaining) {
            for (int i = 0; i < otherSize; i++) {
                if (toAppend[i]) {
                    other.addKeyToSet(resultKeySet, i);
                    other.appendValue(i, resultValueBlockBuilder);
                }
            }
        }

        checkState(resultKeySet.size() == resultValueBlockBuilder.getPositionCount());
        return new KeySetAndValues(keyType, valueType, adder, resultKeySet, resultValueBlockBuilder.build());
    }

    public MapUnionSumResult unionSum(Block mapBlock)
    {
        MapUnionSumResult mapUnionSumResult = new SingleMapBlock(keyType, valueType, adder, mapBlock);
        return unionSum(mapUnionSumResult);
    }

    /**
     * Holds the input map block to avoid unnecessary typeset creation for the first map.
     */
    private static class SingleMapBlock
            extends MapUnionSumResult
    {
        private final Block mapBlock;

        public SingleMapBlock(Type keyType, Type valueType, Adder adder, Block mapBlock)
        {
            super(keyType, valueType, adder);
            this.mapBlock = mapBlock;
        }

        @Override
        int size()
        {
            return mapBlock.getPositionCount() / 2;
        }

        @Override
        void addKeyToSet(TypedSet keySet, int i)
        {
            keySet.add(mapBlock, 2 * i);
        }
        @Override
        void addKey(int i, BlockBuilder out)
        {
            keyType.appendTo(mapBlock, 2 * i, out);
        }

        @Override
        void appendValue(int i, BlockBuilder blockBuilder)
        {
            appendValue(valueType, mapBlock, 2 * i + 1, blockBuilder);
        }

        @Override
        boolean isValueNull(int i)
        {
            return mapBlock.isNull(2 * i + 1);
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE;
        }

        @Override
        int getPosition(TypedSet otherKeySet, int keyPosition)
        {
            return otherKeySet.positionOf(mapBlock, 2 * keyPosition);
        }

        TypedSet getKeySet()
        {
            TypedSet resultKeySet = new TypedSet(keyType, mapBlock.getPositionCount() / 2, "MAP_UNION_SUM");
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                resultKeySet.add(mapBlock, i);
            }

            return resultKeySet;
        }

        @Override
        Block getValueBlock()
        {
            return mapBlock;
        }

        @Override
        int getValueBlockIndex(int i)
        {
            return 2 * i + 1;
        }
    }

    /**
     * Holds the result of aggregating two or more maps.
     */
    private static class KeySetAndValues
            extends MapUnionSumResult
    {
        private final TypedSet keySet;
        private final Block valueBlock;

        KeySetAndValues(Type keyType, Type valueType, Adder adder, TypedSet keySet, Block valueBlock)
        {
            super(keyType, valueType, adder);
            this.keySet = keySet;
            this.valueBlock = valueBlock;
        }

        @Override
        int size()
        {
            return keySet.size();
        }

        @Override
        void addKeyToSet(TypedSet keySet, int i)
        {
            keySet.add(keySet.getBlockBuilder(), i);
        }

        @Override
        void addKey(int i, BlockBuilder out)
        {
            keyType.appendTo(keySet.getBlockBuilder(), i, out);
        }

        @Override
        void appendValue(int i, BlockBuilder blockBuilder)
        {
            appendValue(valueType, valueBlock, i, blockBuilder);
        }

        @Override
        boolean isValueNull(int i)
        {
            return valueBlock.isNull(i);
        }

        @Override
        public long getRetainedSizeInBytes()
        {
            return INSTANCE_SIZE + keySet.getRetainedSizeInBytes() + valueBlock.getRetainedSizeInBytes();
        }

        @Override
        int getPosition(TypedSet otherKeySet, int keyPosition)
        {
            return otherKeySet.positionOf(keySet.getBlockBuilder(), keyPosition);
        }

        TypedSet getKeySet()
        {
            return keySet;
        }

        @Override
        Block getValueBlock()
        {
            return valueBlock;
        }

        @Override
        int getValueBlockIndex(int i)
        {
            return i;
        }
    }
}
