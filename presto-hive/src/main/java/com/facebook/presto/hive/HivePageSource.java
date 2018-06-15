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

import com.facebook.presto.hive.HivePageSourceProvider.BucketAdaptation;
import com.facebook.presto.hive.HivePageSourceProvider.ColumnMapping;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.ArrayBlock;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.ColumnarArray;
import com.facebook.presto.spi.block.ColumnarMap;
import com.facebook.presto.spi.block.ColumnarRow;
import com.facebook.presto.spi.block.DictionaryBlock;
import com.facebook.presto.spi.block.LazyBlock;
import com.facebook.presto.spi.block.LazyBlockLoader;
import com.facebook.presto.spi.block.RowBlock;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.VarcharType;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveBucketing.getHiveBucket;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_BUCKET_FILES;
import static com.facebook.presto.hive.HivePageSourceProvider.ColumnMappingKind.PREFILLED;
import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.hive.HiveUtil.bigintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.booleanPartitionKey;
import static com.facebook.presto.hive.HiveUtil.charPartitionKey;
import static com.facebook.presto.hive.HiveUtil.datePartitionKey;
import static com.facebook.presto.hive.HiveUtil.doublePartitionKey;
import static com.facebook.presto.hive.HiveUtil.extractStructFieldTypes;
import static com.facebook.presto.hive.HiveUtil.floatPartitionKey;
import static com.facebook.presto.hive.HiveUtil.integerPartitionKey;
import static com.facebook.presto.hive.HiveUtil.isArrayType;
import static com.facebook.presto.hive.HiveUtil.isHiveNull;
import static com.facebook.presto.hive.HiveUtil.isMapType;
import static com.facebook.presto.hive.HiveUtil.isRowType;
import static com.facebook.presto.hive.HiveUtil.longDecimalPartitionKey;
import static com.facebook.presto.hive.HiveUtil.shortDecimalPartitionKey;
import static com.facebook.presto.hive.HiveUtil.smallintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.timestampPartitionKey;
import static com.facebook.presto.hive.HiveUtil.tinyintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.varcharPartitionKey;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.block.ColumnarArray.toColumnarArray;
import static com.facebook.presto.spi.block.ColumnarMap.toColumnarMap;
import static com.facebook.presto.spi.block.ColumnarRow.toColumnarRow;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.isLongDecimal;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.intBitsToFloat;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class HivePageSource
        implements ConnectorPageSource
{
    private final List<ColumnMapping> columnMappings;
    private final Optional<BucketAdapter> bucketAdapter;
    private final Object[] prefilledValues;
    private final Type[] types;
    private final Function<Block, Block>[] coercers;

    private final ConnectorPageSource delegate;

    public HivePageSource(
            List<ColumnMapping> columnMappings,
            Optional<BucketAdaptation> bucketAdaptation,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            ConnectorPageSource delegate)
    {
        requireNonNull(columnMappings, "columnMappings is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(typeManager, "typeManager is null");

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.columnMappings = columnMappings;
        this.bucketAdapter = bucketAdaptation.map(BucketAdapter::new);

        int size = columnMappings.size();

        prefilledValues = new Object[size];
        types = new Type[size];
        coercers = new Function[size];

        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            ColumnMapping columnMapping = columnMappings.get(columnIndex);
            HiveColumnHandle column = columnMapping.getHiveColumnHandle();

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());
            types[columnIndex] = type;

            if (columnMapping.getCoercionFrom().isPresent()) {
                coercers[columnIndex] = createCoercer(typeManager, columnMapping.getCoercionFrom().get(), columnMapping.getHiveColumnHandle().getHiveType());
            }

            if (columnMapping.getKind() == PREFILLED) {
                String columnValue = columnMapping.getPrefilledValue();
                byte[] bytes = columnValue.getBytes(UTF_8);

                Object prefilledValue;
                if (isHiveNull(bytes)) {
                    prefilledValue = null;
                }
                else if (type.equals(BOOLEAN)) {
                    prefilledValue = booleanPartitionKey(columnValue, name);
                }
                else if (type.equals(BIGINT)) {
                    prefilledValue = bigintPartitionKey(columnValue, name);
                }
                else if (type.equals(INTEGER)) {
                    prefilledValue = integerPartitionKey(columnValue, name);
                }
                else if (type.equals(SMALLINT)) {
                    prefilledValue = smallintPartitionKey(columnValue, name);
                }
                else if (type.equals(TINYINT)) {
                    prefilledValue = tinyintPartitionKey(columnValue, name);
                }
                else if (type.equals(REAL)) {
                    prefilledValue = floatPartitionKey(columnValue, name);
                }
                else if (type.equals(DOUBLE)) {
                    prefilledValue = doublePartitionKey(columnValue, name);
                }
                else if (isVarcharType(type)) {
                    prefilledValue = varcharPartitionKey(columnValue, name, type);
                }
                else if (isCharType(type)) {
                    prefilledValue = charPartitionKey(columnValue, name, type);
                }
                else if (type.equals(DATE)) {
                    prefilledValue = datePartitionKey(columnValue, name);
                }
                else if (type.equals(TIMESTAMP)) {
                    prefilledValue = timestampPartitionKey(columnValue, hiveStorageTimeZone, name);
                }
                else if (isShortDecimal(type)) {
                    prefilledValue = shortDecimalPartitionKey(columnValue, (DecimalType) type, name);
                }
                else if (isLongDecimal(type)) {
                    prefilledValue = longDecimalPartitionKey(columnValue, (DecimalType) type, name);
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for prefilled column: %s", type.getDisplayName(), name));
                }

                prefilledValues[columnIndex] = prefilledValue;
            }
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        try {
            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }

            if (bucketAdapter.isPresent()) {
                IntArrayList rowsToKeep = bucketAdapter.get().computeEligibleRowIds(dataPage);
                Block[] adaptedBlocks = new Block[dataPage.getChannelCount()];
                for (int i = 0; i < adaptedBlocks.length; i++) {
                    Block block = dataPage.getBlock(i);
                    if (block instanceof LazyBlock && !((LazyBlock) block).isLoaded()) {
                        adaptedBlocks[i] = new LazyBlock(rowsToKeep.size(), new RowFilterLazyBlockLoader(dataPage.getBlock(i), rowsToKeep));
                    }
                    else {
                        adaptedBlocks[i] = block.getPositions(rowsToKeep.elements(), 0, rowsToKeep.size());
                    }
                }
                dataPage = new Page(rowsToKeep.size(), adaptedBlocks);
            }

            int batchSize = dataPage.getPositionCount();
            List<Block> blocks = new ArrayList<>();
            for (int fieldId = 0; fieldId < columnMappings.size(); fieldId++) {
                ColumnMapping columnMapping = columnMappings.get(fieldId);
                switch (columnMapping.getKind()) {
                    case PREFILLED:
                        blocks.add(RunLengthEncodedBlock.create(types[fieldId], prefilledValues[fieldId], batchSize));
                        break;
                    case REGULAR:
                        Block block = dataPage.getBlock(columnMapping.getIndex());
                        if (coercers[fieldId] != null) {
                            block = new LazyBlock(batchSize, new CoercionLazyBlockLoader(block, coercers[fieldId]));
                        }
                        blocks.add(block);
                        break;
                    case INTERIM:
                        // interim columns don't show up in output
                        break;
                    default:
                        throw new UnsupportedOperationException();
                }
            }
            return new Page(batchSize, blocks.toArray(new Block[0]));
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }

    public ConnectorPageSource getPageSource()
    {
        return delegate;
    }

    private static Function<Block, Block> createCoercer(TypeManager typeManager, HiveType fromHiveType, HiveType toHiveType)
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

    private static class IntegerNumberUpscaleCoercer
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

    private static class IntegerNumberToVarcharCoercer
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

    private static class VarcharToIntegerNumberCoercer
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

    private static class FloatToDoubleCoercer
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

    private static class ListCoercer
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
            return ArrayBlock.fromElementBlock(arrayBlock.getPositionCount(), valueIsNull, offsets, elementsBlock);
        }
    }

    private static class MapCoercer
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
            boolean[] valueIsNull = new boolean[mapBlock.getPositionCount()];
            int[] offsets = new int[mapBlock.getPositionCount() + 1];
            for (int i = 0; i < mapBlock.getPositionCount(); i++) {
                valueIsNull[i] = mapBlock.isNull(i);
                offsets[i + 1] = offsets[i] + mapBlock.getEntryCount(i);
            }
            return ((MapType) toType).createBlockFromKeyValue(valueIsNull, offsets, keysBlock, valuesBlock);
        }
    }

    private static class StructCoercer
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
            return RowBlock.fromFieldBlocks(valueIsNull, fields);
        }
    }

    private static final class CoercionLazyBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private final Function<Block, Block> coercer;
        private Block block;

        public CoercionLazyBlockLoader(Block block, Function<Block, Block> coercer)
        {
            this.block = requireNonNull(block, "block is null");
            this.coercer = requireNonNull(coercer, "coercer is null");
        }

        @Override
        public void load(LazyBlock lazyBlock)
        {
            if (block == null) {
                return;
            }

            lazyBlock.setBlock(coercer.apply(block.getLoadedBlock()));

            // clear reference to loader to free resources, since load was successful
            block = null;
        }
    }

    private static final class RowFilterLazyBlockLoader
            implements LazyBlockLoader<LazyBlock>
    {
        private Block block;
        private final IntArrayList rowsToKeep;

        public RowFilterLazyBlockLoader(Block block, IntArrayList rowsToKeep)
        {
            this.block = requireNonNull(block, "block is null");
            this.rowsToKeep = requireNonNull(rowsToKeep, "rowsToKeep is null");
        }

        @Override
        public void load(LazyBlock lazyBlock)
        {
            if (block == null) {
                return;
            }

            lazyBlock.setBlock(block.getPositions(rowsToKeep.elements(), 0, rowsToKeep.size()));

            // clear reference to loader to free resources, since load was successful
            block = null;
        }
    }

    private static Page extractColumns(Page page, int[] columns)
    {
        Block[] blocks = new Block[columns.length];
        for (int i = 0; i < columns.length; i++) {
            int dataColumn = columns[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        return new Page(page.getPositionCount(), blocks);
    }

    public static class BucketAdapter
    {
        public final int[] bucketColumns;
        public final int bucketToKeep;
        public final int tableBucketCount;
        public final int partitionBucketCount; // for sanity check only
        private final List<TypeInfo> typeInfoList;

        public BucketAdapter(BucketAdaptation bucketAdaptation)
        {
            this.bucketColumns = bucketAdaptation.getBucketColumnIndices();
            this.bucketToKeep = bucketAdaptation.getBucketToKeep();
            this.typeInfoList = bucketAdaptation.getBucketColumnHiveTypes().stream()
                    .map(HiveType::getTypeInfo)
                    .collect(toImmutableList());
            this.tableBucketCount = bucketAdaptation.getTableBucketCount();
            this.partitionBucketCount = bucketAdaptation.getPartitionBucketCount();
        }

        public IntArrayList computeEligibleRowIds(Page page)
        {
            IntArrayList ids = new IntArrayList(page.getPositionCount());
            Page bucketColumnsPage = extractColumns(page, bucketColumns);
            for (int position = 0; position < page.getPositionCount(); position++) {
                int bucket = getHiveBucket(tableBucketCount, typeInfoList, bucketColumnsPage, position);
                if ((bucket - bucketToKeep) % partitionBucketCount != 0) {
                    throw new PrestoException(HIVE_INVALID_BUCKET_FILES, format(
                            "A row that is supposed to be in bucket %s is encountered. Only rows in bucket %s (modulo %s) are expected",
                            bucket, bucketToKeep % partitionBucketCount, partitionBucketCount));
                }
                if (bucket == bucketToKeep) {
                    ids.add(position);
                }
            }
            return ids;
        }
    }
}
