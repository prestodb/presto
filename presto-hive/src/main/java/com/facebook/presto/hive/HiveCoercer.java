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

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.block.ArrayBlock;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.block.ColumnarArray;
import com.facebook.presto.common.block.ColumnarMap;
import com.facebook.presto.common.block.ColumnarRow;
import com.facebook.presto.common.block.DictionaryBlock;
import com.facebook.presto.common.block.RowBlock;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.orc.TupleDomainFilter;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.common.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.common.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.common.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.hive.HiveUtil.extractStructFieldNames;
import static com.facebook.presto.hive.HiveUtil.extractStructFieldTypes;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isArrayType;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isMapType;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isRowType;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NOT_NULL;
import static com.facebook.presto.orc.TupleDomainFilter.IS_NULL;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public interface HiveCoercer
        extends Function<Block, Block>
{
    TupleDomainFilter toCoercingFilter(TupleDomainFilter filter, Subfield subfield);

    Type getToType();

    static HiveCoercer createCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
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
            implements HiveCoercer
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

        @Override
        public TupleDomainFilter toCoercingFilter(TupleDomainFilter filter, Subfield subfield)
        {
            checkArgument(subfield.getPath().isEmpty(), "Subfields on primitive types are not allowed");
            return filter;
        }

        @Override
        public Type getToType()
        {
            return toType;
        }
    }

    class IntegerNumberToVarcharCoercer
            implements HiveCoercer
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

        @Override
        public TupleDomainFilter toCoercingFilter(TupleDomainFilter filter, Subfield subfield)
        {
            checkArgument(subfield.getPath().isEmpty(), "Subfields on primitive types are not allowed");
            return new CoercingFilter(filter);
        }

        private static final class CoercingFilter
                extends TupleDomainFilter.AbstractTupleDomainFilter
        {
            private final TupleDomainFilter delegate;

            public CoercingFilter(TupleDomainFilter delegate)
            {
                super(delegate.isDeterministic(), !delegate.isDeterministic() || delegate.testNull());
                this.delegate = requireNonNull(delegate, "delegate is null");
            }

            @Override
            public boolean testNull()
            {
                return delegate.testNull();
            }

            @Override
            public boolean testLong(long value)
            {
                byte[] bytes = String.valueOf(value).getBytes();
                return delegate.testBytes(bytes, 0, bytes.length);
            }
        }

        @Override
        public Type getToType()
        {
            return toType;
        }
    }

    class VarcharToIntegerNumberCoercer
            implements HiveCoercer
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

        @Override
        public TupleDomainFilter toCoercingFilter(TupleDomainFilter filter, Subfield subfield)
        {
            checkArgument(subfield.getPath().isEmpty(), "Subfields on primitive types are not allowed");
            return new CoercingFilter(filter, minValue, maxValue);
        }

        private static final class CoercingFilter
                extends TupleDomainFilter.AbstractTupleDomainFilter
        {
            private final TupleDomainFilter delegate;
            private final long minValue;
            private final long maxValue;

            public CoercingFilter(TupleDomainFilter delegate, long minValue, long maxValue)
            {
                super(delegate.isDeterministic(), !delegate.isDeterministic() || delegate.testNull());
                this.delegate = requireNonNull(delegate, "delegate is null");
                this.minValue = minValue;
                this.maxValue = maxValue;
            }

            @Override
            public boolean testNull()
            {
                return delegate.testNull();
            }

            @Override
            public boolean testLength(int length)
            {
                return true;
            }

            @Override
            public boolean testBytes(byte[] buffer, int offset, int length)
            {
                long value;
                try {
                    value = Long.valueOf(new String(buffer, offset, length));
                }
                catch (NumberFormatException e) {
                    return delegate.testNull();
                }

                if (minValue <= value && value <= maxValue) {
                    return delegate.testLong(value);
                }
                else {
                    return delegate.testNull();
                }
            }
        }

        @Override
        public Type getToType()
        {
            return toType;
        }
    }

    class FloatToDoubleCoercer
            implements HiveCoercer
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

        @Override
        public TupleDomainFilter toCoercingFilter(TupleDomainFilter filter, Subfield subfield)
        {
            checkArgument(subfield.getPath().isEmpty(), "Subfields on primitive types are not allowed");
            return filter;
        }

        @Override
        public Type getToType()
        {
            return DOUBLE;
        }
    }

    class ListCoercer
            implements HiveCoercer
    {
        private final HiveCoercer elementCoercer;
        private final Type toType;

        public ListCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
        {
            requireNonNull(typeManager, "typeManage is null");
            requireNonNull(fromHiveType, "fromHiveType is null");
            requireNonNull(toHiveType, "toHiveType is null");
            HiveType fromElementHiveType = HiveType.valueOf(((ListTypeInfo) fromHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
            HiveType toElementHiveType = HiveType.valueOf(((ListTypeInfo) toHiveType.getTypeInfo()).getListElementTypeInfo().getTypeName());
            this.elementCoercer = fromElementHiveType.equals(toElementHiveType) ? null : createCoercer(typeManager, fromElementHiveType, toElementHiveType);
            this.toType = toHiveType.getType(typeManager);
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

        @Override
        public TupleDomainFilter toCoercingFilter(TupleDomainFilter filter, Subfield subfield)
        {
            if (filter == IS_NULL || filter == IS_NOT_NULL) {
                return filter;
            }

            throw new UnsupportedOperationException("Range filers on array elements are not supported");
        }

        @Override
        public Type getToType()
        {
            return toType;
        }
    }

    class MapCoercer
            implements HiveCoercer
    {
        private final Type toType;
        private final HiveCoercer keyCoercer;
        private final HiveCoercer valueCoercer;

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

        @Override
        public TupleDomainFilter toCoercingFilter(TupleDomainFilter filter, Subfield subfield)
        {
            if (filter == IS_NULL || filter == IS_NOT_NULL) {
                return filter;
            }

            throw new UnsupportedOperationException("Range filers on map elements are not supported");
        }

        @Override
        public Type getToType()
        {
            return toType;
        }
    }

    class StructCoercer
            implements HiveCoercer
    {
        private final HiveCoercer[] coercers;
        private final Block[] nullBlocks;
        private final List<String> toFieldNames;
        private final Type toType;

        public StructCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
        {
            requireNonNull(typeManager, "typeManage is null");
            requireNonNull(fromHiveType, "fromHiveType is null");
            requireNonNull(toHiveType, "toHiveType is null");
            List<HiveType> fromFieldTypes = extractStructFieldTypes(fromHiveType);
            List<HiveType> toFieldTypes = extractStructFieldTypes(toHiveType);
            this.coercers = new HiveCoercer[toFieldTypes.size()];
            this.nullBlocks = new Block[toFieldTypes.size()];
            for (int i = 0; i < coercers.length; i++) {
                if (i >= fromFieldTypes.size()) {
                    nullBlocks[i] = toFieldTypes.get(i).getType(typeManager).createBlockBuilder(null, 1).appendNull().build();
                }
                else if (!fromFieldTypes.get(i).equals(toFieldTypes.get(i))) {
                    coercers[i] = createCoercer(typeManager, fromFieldTypes.get(i), toFieldTypes.get(i));
                }
            }
            this.toFieldNames = extractStructFieldNames(toHiveType);
            this.toType = toHiveType.getType(typeManager);
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

        @Override
        public TupleDomainFilter toCoercingFilter(TupleDomainFilter filter, Subfield subfield)
        {
            if (filter == IS_NULL || filter == IS_NOT_NULL) {
                return filter;
            }

            if (subfield.getPath().size() > 0) {
                String fieldName = ((Subfield.NestedField) subfield.getPath().get(0)).getName();
                for (int i = 0; i < toFieldNames.size(); i++) {
                    if (fieldName.equals(toFieldNames.get(i))) {
                        HiveCoercer coercer = coercers[i];
                        if (coercer == null) {
                            // the column value will be null
                            //  -> only isNull method will be called
                            //   -> the original filter will work just fine
                            return filter;
                        }
                        return coercer.toCoercingFilter(filter, subfield.tail(fieldName));
                    }
                }
                throw new IllegalArgumentException("Struct field not found: " + fieldName);
            }

            throw new UnsupportedOperationException("Range filers on struct types are not supported");
        }

        @Override
        public Type getToType()
        {
            return toType;
        }
    }
}
