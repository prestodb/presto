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
package com.facebook.presto.hive;

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.ColumnarArray;
import com.facebook.presto.spi.block.ColumnarMap;
import com.facebook.presto.spi.block.ColumnarRow;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.RowBlock;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.hive.HiveUtil.extractStructFieldTypes;
import static com.facebook.presto.hive.HiveUtil.isArrayType;
import static com.facebook.presto.hive.HiveUtil.isMapType;
import static com.facebook.presto.hive.HiveUtil.isRowType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.spi.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.spi.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public interface HiveCoercers
{
    static Function<Block, Block> createCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
    {
        Type fromType = typeManager.getType(fromHiveType.getTypeSignature());
        Type toType = typeManager.getType(toHiveType.getTypeSignature());
        if (toType instanceof VarcharType && (fromHiveType.equals(HIVE_BYTE) || fromHiveType.equals(HIVE_SHORT) || fromHiveType.equals(HIVE_INT) || fromHiveType.equals(HIVE_LONG))) {
            return new IntegerNumberToVarcharCoercer(fromType, toType);
        }
        else if (fromType instanceof VarcharType && (toHiveType.equals(HIVE_BYTE) || toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG))) {
            return new VarcharToIntegerNumberCoercer(fromType, toType);
        }
        else if (fromHiveType.equals(HIVE_BYTE) && toHiveType.equals(HIVE_SHORT) || toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer(fromType, toType);
        }
        else if (fromHiveType.equals(HIVE_SHORT) && toHiveType.equals(HIVE_INT) || toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer(fromType, toType);
        }
        else if (fromHiveType.equals(HIVE_INT) && toHiveType.equals(HIVE_LONG)) {
            return new IntegerNumberUpscaleCoercer(fromType, toType);
        }
        else if (fromHiveType.equals(HIVE_FLOAT) && toHiveType.equals(HIVE_DOUBLE)) {
            return new FloatToDoubleCoercer();
        }
        else if (isArrayType(fromType) && isArrayType(toType)) {
            return new ListCoercer(typeManager, fromHiveType, toHiveType);
        }
        else if (isMapType(fromType) && isMapType(toType)) {
            return new MapCoercer(typeManager, fromHiveType, toHiveType);
        }
        else if (isRowType(fromType) && isRowType(toType)) {
            return new StructCoercer(typeManager, fromHiveType, toHiveType);
        }

        throw new PrestoException(NOT_SUPPORTED, format("Unsupported coercion from %s to %s", fromHiveType, toHiveType));
    }

    class IntegerNumberUpscaleCoercer
            implements Function<Block, Block>
    {
        private final Type fromType;
        private final Type toType;

        public IntegerNumberUpscaleCoercer(Type fromType, Type toType)
        {
            this.fromType = requireNonNull(fromType, "fromType is null");
            this.toType = requireNonNull(toType, "toType is null");
        }

        @Override
        public Block apply(Block block)
        {
            BlockBuilder blockBuilder = toType.createBlockBuilder(null, block.getPositionCount());
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    blockBuilder.appendNull();
                    continue;
                }
                toType.writeLong(blockBuilder, fromType.getLong(block, i));
            }
            return blockBuilder.build();
        }
    }

    class IntegerNumberToVarcharCoercer
            implements Function<Block, Block>
    {
        private final Type fromType;
        private final Type toType;

        public IntegerNumberToVarcharCoercer(Type fromType, Type toType)
        {
            this.fromType = requireNonNull(fromType, "fromType is null");
            this.toType = requireNonNull(toType, "toType is null");
        }

        @Override
        public Block apply(Block block)
        {
            BlockBuilder blockBuilder = toType.createBlockBuilder(null, block.getPositionCount());
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    blockBuilder.appendNull();
                    continue;
                }
                toType.writeSlice(blockBuilder, utf8Slice(String.valueOf(fromType.getLong(block, i))));
            }
            return blockBuilder.build();
        }
    }

    class VarcharToIntegerNumberCoercer
            implements Function<Block, Block>
    {
        private final Type fromType;
        private final Type toType;

        private final long minValue;
        private final long maxValue;

        public VarcharToIntegerNumberCoercer(Type fromType, Type toType)
        {
            this.fromType = requireNonNull(fromType, "fromType is null");
            this.toType = requireNonNull(toType, "toType is null");

            if (toType.equals(TINYINT)) {
                minValue = Byte.MIN_VALUE;
                maxValue = Byte.MAX_VALUE;
            }
            else if (toType.equals(SMALLINT)) {
                minValue = Short.MIN_VALUE;
                maxValue = Short.MAX_VALUE;
            }
            else if (toType.equals(INTEGER)) {
                minValue = Integer.MIN_VALUE;
                maxValue = Integer.MAX_VALUE;
            }
            else if (toType.equals(BIGINT)) {
                minValue = Long.MIN_VALUE;
                maxValue = Long.MAX_VALUE;
            }
            else {
                throw new PrestoException(NOT_SUPPORTED, format("Could not create Coercer from from varchar to %s", toType));
            }
        }

        @Override
        public Block apply(Block block)
        {
            BlockBuilder blockBuilder = toType.createBlockBuilder(null, block.getPositionCount());
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    blockBuilder.appendNull();
                    continue;
                }
                try {
                    long value = Long.parseLong(fromType.getSlice(block, i).toStringUtf8());
                    if (minValue <= value && value <= maxValue) {
                        toType.writeLong(blockBuilder, value);
                    }
                    else {
                        blockBuilder.appendNull();
                    }
                }
                catch (NumberFormatException e) {
                    blockBuilder.appendNull();
                }
            }
            return blockBuilder.build();
        }
    }

    class FloatToDoubleCoercer
            implements Function<Block, Block>
    {
        @Override
        public Block apply(Block block)
        {
            BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, block.getPositionCount());
            for (int i = 0; i < block.getPositionCount(); i++) {
                if (block.isNull(i)) {
                    blockBuilder.appendNull();
                    continue;
                }
                DOUBLE.writeDouble(blockBuilder, intBitsToFloat((int) REAL.getLong(block, i)));
            }
            return blockBuilder.build();
        }
    }

    class ListCoercer
            implements Function<Block, Block>
    {
        private final Function<Block, Block> elementCoercer;

        public ListCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
        {
            requireNonNull(typeManager, "typeManage is null");
            requireNonNull(fromHiveType, "fromHiveType is null");
            requireNonNull(toHiveType, "toHiveType is null");
            HiveType fromElementHiveType = HiveType.valueOf(((ListTypeInfo) fromHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
            HiveType toElementHiveType = HiveType.valueOf(((ListTypeInfo) toHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
            this.elementCoercer = fromElementHiveType.equals(toElementHiveType) ? null : createCoercer(typeManager, fromElementHiveType, toElementHiveType);
        }

        @Override
        public Block apply(Block block)
        {
            if (elementCoercer == null) {
                return block;
            }
            ColumnarArray arrayBlock = toColumnarArray(block);
            Block elementsBlock = elementCoercer.apply(arrayBlock.getElementsBlock());
            boolean[] valueIsNull = new boolean[arrayBlock.getPositionCount()];
            int[] offsets = new int[arrayBlock.getPositionCount() + 1];
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                valueIsNull[i] = arrayBlock.isNull(i);
                offsets[i + 1] = offsets[i] + arrayBlock.getLength(i);
            }
            return ArrayBlock.fromElementBlock(arrayBlock.getPositionCount(), Optional.of(valueIsNull), offsets, elementsBlock);
        }
    }

    class MapCoercer
            implements Function<Block, Block>
    {
        private final Type toType;
        private final Function<Block, Block> keyCoercer;
        private final Function<Block, Block> valueCoercer;

        public MapCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
        {
            requireNonNull(typeManager, "typeManage is null");
            requireNonNull(fromHiveType, "fromHiveType is null");
            this.toType = requireNonNull(toHiveType, "toHiveType is null").getType(typeManager);
            HiveType fromKeyHiveType = HiveType.valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
            HiveType fromValueHiveType = HiveType.valueOf(((MapTypeInfo) fromHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
            HiveType toKeyHiveType = HiveType.valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapKeyTypeInfo().getTypeName());
            HiveType toValueHiveType = HiveType.valueOf(((MapTypeInfo) toHiveType.getTypeInfo()).getMapValueTypeInfo().getTypeName());
            this.keyCoercer = fromKeyHiveType.equals(toKeyHiveType) ? null : createCoercer(typeManager, fromKeyHiveType, toKeyHiveType);
            this.valueCoercer = fromValueHiveType.equals(toValueHiveType) ? null : createCoercer(typeManager, fromValueHiveType, toValueHiveType);
        }

        @Override
        public Block apply(Block block)
        {
            ColumnarMap mapBlock = toColumnarMap(block);
            Block keysBlock = keyCoercer == null ? mapBlock.getKeysBlock() : keyCoercer.apply(mapBlock.getKeysBlock());
            Block valuesBlock = valueCoercer == null ? mapBlock.getValuesBlock() : valueCoercer.apply(mapBlock.getValuesBlock());
            int positionCount = mapBlock.getPositionCount();
            boolean[] valueIsNull = new boolean[positionCount];
            int[] offsets = new int[positionCount + 1];
            for (int i = 0; i < positionCount; i++) {
                valueIsNull[i] = mapBlock.isNull(i);
                offsets[i + 1] = offsets[i] + mapBlock.getEntryCount(i);
            }
            return ((MapType) toType).createBlockFromKeyValue(positionCount, Optional.of(valueIsNull), offsets, keysBlock, valuesBlock);
        }
    }

    class StructCoercer
            implements Function<Block, Block>
    {
        private final Function<Block, Block>[] coercers;
        private final Block[] nullBlocks;

        public StructCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
        {
            requireNonNull(typeManager, "typeManage is null");
            requireNonNull(fromHiveType, "fromHiveType is null");
            requireNonNull(toHiveType, "toHiveType is null");
            List<HiveType> fromFieldTypes = extractStructFieldTypes(fromHiveType);
            List<HiveType> toFieldTypes = extractStructFieldTypes(toHiveType);
            this.coercers = new Function[toFieldTypes.size()];
            this.nullBlocks = new Block[toFieldTypes.size()];
            for (int i = 0; i < coercers.length; i++) {
                if (i >= fromFieldTypes.size()) {
                    nullBlocks[i] = toFieldTypes.get(i).getType(typeManager).createBlockBuilder(null, 1).appendNull().build();
                }
                else if (!fromFieldTypes.get(i).equals(toFieldTypes.get(i))) {
                    coercers[i] = createCoercer(typeManager, fromFieldTypes.get(i), toFieldTypes.get(i));
                }
            }
        }

        @Override
        public Block apply(Block block)
        {
            ColumnarRow rowBlock = toColumnarRow(block);
            Block[] fields = new Block[coercers.length];
            int[] ids = new int[rowBlock.getField(0).getPositionCount()];
            for (int i = 0; i < coercers.length; i++) {
                if (coercers[i] != null) {
                    fields[i] = coercers[i].apply(rowBlock.getField(i));
                }
                else if (i < rowBlock.getFieldCount()) {
                    fields[i] = rowBlock.getField(i);
                }
                else {
                    fields[i] = new DictionaryBlock(nullBlocks[i], ids);
                }
            }
            boolean[] valueIsNull = new boolean[rowBlock.getPositionCount()];
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                valueIsNull[i] = rowBlock.isNull(i);
            }
            return RowBlock.fromFieldBlocks(valueIsNull.length, Optional.of(valueIsNull), fields);
        }
    }
}
