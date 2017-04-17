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
package com.facebook.presto.hive.parquet;

import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.hive.parquet.predicate.ParquetPredicate;
import com.facebook.presto.hive.util.DecimalUtils;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.block.BlockBuilderStatus;
import com.facebook.presto.spi.block.InterleavedBlockBuilder;
import com.facebook.presto.spi.predicate.TupleDomain;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.NamedTypeSignature;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import parquet.column.ColumnDescriptor;
import parquet.column.Dictionary;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.ParquetRecordReader;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hadoop.util.ContextUtil;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.DecimalMetadata;
import parquet.schema.GroupType;
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.presto.hive.HiveColumnHandle.ColumnType.REGULAR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_MISSING_DATA;
import static com.facebook.presto.hive.HiveUtil.closeWithSuppression;
import static com.facebook.presto.hive.HiveUtil.getDecimalType;
import static com.facebook.presto.hive.parquet.HdfsParquetDataSource.buildHdfsParquetDataSource;
import static com.facebook.presto.hive.parquet.ParquetTypeUtils.getParquetType;
import static com.facebook.presto.hive.parquet.predicate.ParquetPredicateUtils.buildParquetPredicate;
import static com.facebook.presto.hive.parquet.predicate.ParquetPredicateUtils.getParquetTupleDomain;
import static com.facebook.presto.hive.parquet.predicate.ParquetPredicateUtils.predicateMatches;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.Chars.trimSpacesAndTruncateToLength;
import static com.facebook.presto.spi.type.DecimalType.createDecimalType;
import static com.facebook.presto.spi.type.StandardTypes.ARRAY;
import static com.facebook.presto.spi.type.StandardTypes.MAP;
import static com.facebook.presto.spi.type.StandardTypes.ROW;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Throwables.throwIfInstanceOf;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static parquet.format.converter.ParquetMetadataConverter.NO_FILTER;
import static parquet.schema.OriginalType.DECIMAL;
import static parquet.schema.OriginalType.MAP_KEY_VALUE;

public class ParquetHiveRecordCursor
        implements RecordCursor
{
    private final ParquetRecordReader<FakeParquetRecord> recordReader;

    private final Type[] types;

    private final boolean[] booleans;
    private final long[] longs;
    private final double[] doubles;
    private final Slice[] slices;
    private final Object[] objects;
    private final boolean[] nulls;

    private final long totalBytes;
    private long completedBytes;
    private boolean closed;

    public ParquetHiveRecordCursor(
            HdfsEnvironment hdfsEnvironment,
            String sessionUser,
            Configuration configuration,
            Path path,
            long start,
            long length,
            Properties splitSchema,
            List<HiveColumnHandle> columns,
            boolean useParquetColumnNames,
            TypeManager typeManager,
            boolean predicatePushdownEnabled,
            TupleDomain<HiveColumnHandle> effectivePredicate)
    {
        requireNonNull(path, "path is null");
        checkArgument(length >= 0, "length is negative");
        requireNonNull(splitSchema, "splitSchema is null");
        requireNonNull(columns, "columns is null");

        this.totalBytes = length;

        int size = columns.size();

        this.types = new Type[size];

        this.booleans = new boolean[size];
        this.longs = new long[size];
        this.doubles = new double[size];
        this.slices = new Slice[size];
        this.objects = new Object[size];
        this.nulls = new boolean[size];

        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            checkState(column.getColumnType() == REGULAR, "column type must be regular");

            types[columnIndex] = typeManager.getType(column.getTypeSignature());
        }

        this.recordReader = createParquetRecordReader(
                hdfsEnvironment,
                sessionUser,
                configuration,
                path,
                start,
                length,
                columns,
                useParquetColumnNames,
                predicatePushdownEnabled,
                effectivePredicate,
                typeManager
        );
    }

    @Override
    public long getTotalBytes()
    {
        return totalBytes;
    }

    @Override
    public long getCompletedBytes()
    {
        if (!closed) {
            updateCompletedBytes();
        }
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    private void updateCompletedBytes()
    {
        try {
            long newCompletedBytes = (long) (totalBytes * recordReader.getProgress());
            completedBytes = min(totalBytes, max(completedBytes, newCompletedBytes));
        }
        catch (IOException ignored) {
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public Type getType(int field)
    {
        return types[field];
    }

    @Override
    public boolean advanceNextPosition()
    {
        try {
            // reset null flags
            Arrays.fill(nulls, true);

            if (closed || !recordReader.nextKeyValue()) {
                close();
                return false;
            }

            return true;
        }
        catch (IOException | RuntimeException | InterruptedException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }

            closeWithSuppression(this, e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public boolean getBoolean(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, boolean.class);
        return booleans[fieldId];
    }

    @Override
    public long getLong(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, long.class);
        return longs[fieldId];
    }

    @Override
    public double getDouble(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, double.class);
        return doubles[fieldId];
    }

    @Override
    public Slice getSlice(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, Slice.class);
        return slices[fieldId];
    }

    @Override
    public Object getObject(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, Block.class);
        return objects[fieldId];
    }

    @Override
    public boolean isNull(int fieldId)
    {
        checkState(!closed, "Cursor is closed");
        return nulls[fieldId];
    }

    private void validateType(int fieldId, Class<?> javaType)
    {
        if (types[fieldId].getJavaType() != javaType) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(format("Expected field to be %s, actual %s (field %s)", javaType.getName(), types[fieldId].getJavaType().getName(), fieldId));
        }
    }

    @Override
    public void close()
    {
        // some hive input formats are broken and bad things can happen if you close them multiple times
        if (closed) {
            return;
        }
        closed = true;

        updateCompletedBytes();

        try {
            recordReader.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private ParquetRecordReader<FakeParquetRecord> createParquetRecordReader(
            HdfsEnvironment hdfsEnvironment,
            String sessionUser,
            Configuration configuration,
            Path path,
            long start,
            long length,
            List<HiveColumnHandle> columns,
            boolean useParquetColumnNames,
            boolean predicatePushdownEnabled,
            TupleDomain<HiveColumnHandle> effectivePredicate,
            TypeManager typeManager)
    {
        ParquetDataSource dataSource = null;
        try {
            FileSystem fileSystem = hdfsEnvironment.getFileSystem(sessionUser, path, configuration);
            dataSource = buildHdfsParquetDataSource(fileSystem, path, start, length);
            ParquetMetadata parquetMetadata = hdfsEnvironment.doAs(sessionUser, () -> ParquetFileReader.readFooter(configuration, path, NO_FILTER));
            List<BlockMetaData> blocks = parquetMetadata.getBlocks();
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();
            MessageType fileSchema = fileMetaData.getSchema();

            PrestoReadSupport readSupport = new PrestoReadSupport(useParquetColumnNames, columns, fileSchema, typeManager);

            List<parquet.schema.Type> fields = columns.stream()
                    .filter(column -> column.getColumnType() == REGULAR)
                    .map(column -> getParquetType(column, fileSchema, useParquetColumnNames))
                    .filter(Objects::nonNull)
                    .collect(toList());

            MessageType requestedSchema = new MessageType(fileSchema.getName(), fields);

            LongArrayList offsets = new LongArrayList(blocks.size());
            for (BlockMetaData block : blocks) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= start && firstDataPage < start + length) {
                    if (predicatePushdownEnabled) {
                        TupleDomain<ColumnDescriptor> parquetTupleDomain = getParquetTupleDomain(fileSchema, requestedSchema, effectivePredicate);
                        ParquetPredicate parquetPredicate = buildParquetPredicate(requestedSchema, parquetTupleDomain, fileSchema);
                        if (predicateMatches(parquetPredicate, block, dataSource, fileSchema, requestedSchema, parquetTupleDomain)) {
                            offsets.add(block.getStartingPos());
                        }
                    }
                    else {
                        offsets.add(block.getStartingPos());
                    }
                }
            }

            ParquetInputSplit split = new ParquetInputSplit(path, start, start + length, length, null, offsets.toLongArray());

            TaskAttemptContext taskContext = ContextUtil.newTaskAttemptContext(configuration, new TaskAttemptID());

            return hdfsEnvironment.doAs(sessionUser, () -> {
                ParquetRecordReader<FakeParquetRecord> realReader = new PrestoParquetRecordReader(readSupport);
                realReader.initialize(split, taskContext);
                return realReader;
            });
        }
        catch (Exception e) {
            throwIfInstanceOf(e, PrestoException.class);
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
                throw Throwables.propagate(e);
            }
            String message = format("Error opening Hive split %s (offset=%s, length=%s): %s", path, start, length, e.getMessage());
            if (e.getClass().getSimpleName().equals("BlockMissingException")) {
                throw new PrestoException(HIVE_MISSING_DATA, message, e);
            }
            throw new PrestoException(HIVE_CANNOT_OPEN_SPLIT, message, e);
        }
        finally {
            if (dataSource != null) {
                try {
                    dataSource.close();
                }
                catch (IOException ignored) {
                }
            }
        }
    }

    public class PrestoParquetRecordReader
            extends ParquetRecordReader<FakeParquetRecord>
    {
        public PrestoParquetRecordReader(PrestoReadSupport readSupport)
        {
            super(readSupport);
        }
    }

    public final class PrestoReadSupport
            extends ReadSupport<FakeParquetRecord>
    {
        private final boolean useParquetColumnNames;
        private final List<HiveColumnHandle> columns;
        private final List<Converter> converters;

        public PrestoReadSupport(boolean useParquetColumnNames, List<HiveColumnHandle> columns, MessageType messageType, TypeManager typeManager)
        {
            this.columns = columns;
            this.useParquetColumnNames = useParquetColumnNames;

            ImmutableList.Builder<Converter> converters = ImmutableList.builder();
            for (int i = 0; i < columns.size(); i++) {
                HiveColumnHandle column = columns.get(i);
                if (column.getColumnType() == REGULAR) {
                    parquet.schema.Type parquetType = getParquetType(column, messageType, useParquetColumnNames);
                    if (parquetType == null) {
                        continue;
                    }
                    if (parquetType.isPrimitive()) {
                        Optional<DecimalType> decimalType = getDecimalType(column.getHiveType());
                        if (decimalType.isPresent()) {
                            converters.add(new ParquetDecimalColumnConverter(i, decimalType.get()));
                        }
                        else {
                            converters.add(new ParquetPrimitiveColumnConverter(i));
                        }
                    }
                    else {
                        converters.add(new ParquetColumnConverter(createGroupConverter(types[i], parquetType.getName(), parquetType, i, useParquetColumnNames, typeManager), i));
                    }
                }
            }
            this.converters = converters.build();
        }

        @Override
        @SuppressWarnings("deprecation")
        public ReadContext init(
                Configuration configuration,
                Map<String, String> keyValueMetaData,
                MessageType messageType)
        {
            List<parquet.schema.Type> fields = columns.stream()
                    .filter(column -> column.getColumnType() == REGULAR)
                    .map(column -> getParquetType(column, messageType, useParquetColumnNames))
                    .filter(Objects::nonNull)
                    .collect(toList());
            MessageType requestedProjection = new MessageType(messageType.getName(), fields);
            return new ReadContext(requestedProjection);
        }

        @Override
        public RecordMaterializer<FakeParquetRecord> prepareForRead(
                Configuration configuration,
                Map<String, String> keyValueMetaData,
                MessageType fileSchema,
                ReadContext readContext)
        {
            return new ParquetRecordConverter(converters);
        }
    }

    private static class ParquetRecordConverter
            extends RecordMaterializer<FakeParquetRecord>
    {
        private final ParquetGroupConverter groupConverter;

        public ParquetRecordConverter(List<Converter> converters)
        {
            groupConverter = new ParquetGroupConverter(converters);
        }

        @Override
        public FakeParquetRecord getCurrentRecord()
        {
            // Parquet skips the record if it is null, so we need non-null record
            return FakeParquetRecord.MATERIALIZE_RECORD;
        }

        @Override
        public GroupConverter getRootConverter()
        {
            return groupConverter;
        }
    }

    private enum FakeParquetRecord
    {
        MATERIALIZE_RECORD
    }

    public static class ParquetGroupConverter
            extends GroupConverter
    {
        private final List<Converter> converters;

        public ParquetGroupConverter(List<Converter> converters)
        {
            this.converters = converters;
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return converters.get(fieldIndex);
        }

        @Override
        public void start()
        {
        }

        @Override
        public void end()
        {
        }
    }

    @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
    private class ParquetPrimitiveColumnConverter
            extends PrimitiveConverter
    {
        private final int fieldIndex;

        private ParquetPrimitiveColumnConverter(int fieldIndex)
        {
            this.fieldIndex = fieldIndex;
        }

        @Override
        public boolean isPrimitive()
        {
            return true;
        }

        @Override
        public PrimitiveConverter asPrimitiveConverter()
        {
            return this;
        }

        @Override
        public boolean hasDictionarySupport()
        {
            return false;
        }

        @Override
        public void setDictionary(Dictionary dictionary)
        {
        }

        @Override
        public void addValueFromDictionary(int dictionaryId)
        {
        }

        @Override
        public void addBoolean(boolean value)
        {
            nulls[fieldIndex] = false;
            booleans[fieldIndex] = value;
        }

        @Override
        public void addDouble(double value)
        {
            nulls[fieldIndex] = false;
            doubles[fieldIndex] = value;
        }

        @Override
        public void addLong(long value)
        {
            nulls[fieldIndex] = false;
            longs[fieldIndex] = value;
        }

        @Override
        public void addBinary(Binary value)
        {
            nulls[fieldIndex] = false;
            Type type = types[fieldIndex];
            if (type == TIMESTAMP) {
                longs[fieldIndex] = ParquetTimestampUtils.getTimestampMillis(value);
            }
            else if (isVarcharType(type)) {
                slices[fieldIndex] = truncateToLength(wrappedBuffer(value.getBytes()), type);
            }
            else if (isCharType(type)) {
                slices[fieldIndex] = trimSpacesAndTruncateToLength(wrappedBuffer(value.getBytes()), type);
            }
            else {
                slices[fieldIndex] = wrappedBuffer(value.getBytes());
            }
        }

        @Override
        public void addFloat(float value)
        {
            nulls[fieldIndex] = false;
            longs[fieldIndex] = floatToRawIntBits(value);
        }

        @Override
        public void addInt(int value)
        {
            nulls[fieldIndex] = false;
            longs[fieldIndex] = value;
        }
    }

    // todo: support for other types of decimal storage (see https://github.com/Parquet/parquet-format/blob/master/LogicalTypes.md)
    private class ParquetDecimalColumnConverter
            extends PrimitiveConverter
    {
        private final int fieldIndex;
        private final DecimalType decimalType;

        private ParquetDecimalColumnConverter(int fieldIndex, DecimalType decimalType)
        {
            this.fieldIndex = fieldIndex;
            this.decimalType = requireNonNull(decimalType, "decimalType is null");
        }

        public void addBinary(Binary value)
        {
            nulls[fieldIndex] = false;
            if (decimalType.isShort()) {
                longs[fieldIndex] = DecimalUtils.getShortDecimalValue(value.getBytes());
            }
            else {
                slices[fieldIndex] = Decimals.encodeUnscaledValue(new BigInteger(value.getBytes()));
            }
        }
    }

    public class ParquetColumnConverter
            extends GroupConverter
    {
        private final GroupedConverter groupedConverter;
        private final int fieldIndex;

        public ParquetColumnConverter(GroupedConverter groupedConverter, int fieldIndex)
        {
            this.groupedConverter = groupedConverter;
            this.fieldIndex = fieldIndex;
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return groupedConverter.getConverter(fieldIndex);
        }

        @Override
        public void start()
        {
            groupedConverter.beforeValue(null);
            groupedConverter.start();
        }

        @Override
        public void end()
        {
            groupedConverter.afterValue();
            groupedConverter.end();

            nulls[fieldIndex] = false;

            objects[fieldIndex] = groupedConverter.getBlock();
        }
    }

    private interface BlockConverter
    {
        void beforeValue(BlockBuilder builder);

        void afterValue();
    }

    private interface IndexedBlockConverter extends BlockConverter
    {
        void setFieldIndex(int fieldIndex);
        int getFieldIndex();
        Type getType();
    }

    private abstract static class GroupedConverter
            extends GroupConverter
            implements IndexedBlockConverter
    {
        public abstract Block getBlock();
    }

    private static IndexedBlockConverter createConverter(Type prestoType, String columnName, parquet.schema.Type parquetType, int fieldIndex, boolean useNames, TypeManager typeManager)
    {
        if (parquetType.isPrimitive()) {
            if (parquetType.getOriginalType() == DECIMAL) {
                DecimalMetadata decimalMetadata = ((PrimitiveType) parquetType).getDecimalMetadata();
                return new ParquetDecimalConverter(createDecimalType(decimalMetadata.getPrecision(), decimalMetadata.getScale()), fieldIndex);
            }
            else {
                return new ParquetPrimitiveConverter(prestoType, fieldIndex);
            }
        }

        return createGroupConverter(prestoType, columnName, parquetType, fieldIndex, useNames, typeManager);
    }

    private static GroupedConverter createGroupConverter(Type prestoType, String columnName, parquet.schema.Type parquetType, int fieldIndex, boolean useNames, TypeManager typeManager)
    {
        GroupType groupType = parquetType.asGroupType();
        switch (prestoType.getTypeSignature().getBase()) {
            case ARRAY:
                return new ParquetListConverter(prestoType, columnName, groupType, fieldIndex, useNames, typeManager);
            case MAP:
                return new ParquetMapConverter(prestoType, columnName, groupType, fieldIndex, useNames, typeManager);
            case ROW:
                return new ParquetStructConverter(prestoType, columnName, groupType, fieldIndex, useNames, typeManager);
            default:
                throw new IllegalArgumentException("Column " + columnName + " type " + parquetType.getOriginalType() + " not supported");
        }
    }

    private static class FieldInformation
    {
        private final Type type;
        private final int index;

        private FieldInformation(int index, Type type)
        {
            this.index = index;
            this.type = type;
        }

        static FieldInformation of(int index, Type type)
        {
            return new FieldInformation(index, type);
        }

        public Type getType()
        {
            return type;
        }

        public int getIndex()
        {
            return index;
        }
    }

    private static class ParquetStructConverter
            extends GroupedConverter
    {
        private static final int NULL_BUILDER_POSITIONS_THRESHOLD = 100;
        private static final int NULL_BUILDER_SIZE_IN_BYTES_THRESHOLD = 32768;

        private final Type rowType;
        private int fieldIndex;

        private final List<IndexedBlockConverter> converters;
        private BlockBuilder builder;
        private BlockBuilder nullBuilder; // used internally when builder is set to null
        private BlockBuilder currentEntryBuilder;

        private int[] converterIndexes;
        private List<Type> outOfOrderFieldTypes;
        private InterleavedBlockBuilder outOfOrderBlockBuilder; // a tmp builder for fields out of order

        private List<IndexedBlockConverter> createConverters(String columnName, List<Type> prestoTypeParameters, List<parquet.schema.Type> fieldTypes, TypeManager typeManager)
        {
            ImmutableList.Builder<IndexedBlockConverter> converters = ImmutableList.builder();
            for (int i = 0, n = prestoTypeParameters.size(); i < n; i++) {
                parquet.schema.Type fieldType = fieldTypes.get(i);
                converters.add(createConverter(prestoTypeParameters.get(i), columnName + "." + fieldType.getName(), fieldType, i, false, typeManager));
            }
            return converters.build();
        }

        private List<IndexedBlockConverter> createConvertersByName(String columnName, Type prestoType, List<parquet.schema.Type> fieldTypes, TypeManager typeManager)
        {
            int parquetFieldCount = fieldTypes.size();
            int prestoFieldCount = prestoType.getTypeSignature().getParameters().size();

            Map<String, FieldInformation> fieldTypeMap = new HashMap<>(prestoFieldCount);
            for (int i = 0; i < prestoFieldCount; i++) {
                NamedTypeSignature namedTypeSignature = prestoType.getTypeSignature().getParameters().get(i).getNamedTypeSignature();
                fieldTypeMap.put(namedTypeSignature.getName().toLowerCase(), FieldInformation.of(i, typeManager.getType(namedTypeSignature.getTypeSignature())));
            }

            boolean outOfOrder = false;
            ImmutableList.Builder<IndexedBlockConverter> builder = ImmutableList.builder();
            for (int i = 0, prevFieldIndex = -1; i < parquetFieldCount; i++) {
                parquet.schema.Type fieldType = fieldTypes.get(i);
                FieldInformation fieldInformation = fieldTypeMap.get(fieldType.getName().toLowerCase());
                if (fieldInformation == null) {
                    builder.add(fieldType.isPrimitive() ? ParquetDummyPrimitiveConverter.converter : new ParquetDummyGroupConverter(fieldType));
                }
                else {
                    if (prevFieldIndex > fieldInformation.getIndex()) {
                        outOfOrder = true;
                    }
                    builder.add(createConverter(fieldInformation.getType(), columnName + "." + fieldType.getName(), fieldType, fieldInformation.getIndex(), true, typeManager));
                    prevFieldIndex = fieldInformation.getIndex();
                }
            }
            // When reading parquet files, we need to pass converters and parquet types in parquet schema fields order.
            // After getting the parquet values, we need to build presto blocks in metastore schema fields order.
            List<IndexedBlockConverter> converters = builder.build();
            if (outOfOrder) {
                ImmutableList.Builder<Type> types = ImmutableList.builder();
                converterIndexes = new int[parquetFieldCount];
                for (int i = 0, tmpIndex = 0; i < parquetFieldCount; i++) {
                    IndexedBlockConverter converter = converters.get(i);
                    converterIndexes[i] = converter.getFieldIndex();
                    if (converterIndexes[i] < 0) {
                        continue;
                    }
                    converter.setFieldIndex(tmpIndex++);
                    types.add(converter.getType());
                }
                outOfOrderFieldTypes = types.build();
            }
            return converters;
        }

        public ParquetStructConverter(Type prestoType, String columnName, GroupType entryType, int fieldIndex, boolean useNames, TypeManager typeManager)
        {
            checkArgument(ROW.equals(prestoType.getTypeSignature().getBase()));
            List<parquet.schema.Type> fieldTypes = entryType.getFields();

            if (useNames || fieldTypes.size() != prestoType.getTypeSignature().getParameters().size()) {
                this.converters = createConvertersByName(columnName, prestoType, fieldTypes, typeManager);
            }
            else {
                this.converters = createConverters(columnName, prestoType.getTypeParameters(), fieldTypes, typeManager);
            }

            this.rowType = prestoType;
            this.fieldIndex = fieldIndex;
        }

        @Override
        public Type getType()
        {
            return rowType;
        }

        @Override
        public void setFieldIndex(int fieldIndex)
        {
            this.fieldIndex = fieldIndex;
        }

        @Override
        public int getFieldIndex()
        {
            return fieldIndex;
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return (Converter) converters.get(fieldIndex);
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
            this.builder = builder;
            if (outOfOrderFieldTypes != null) {
                outOfOrderBlockBuilder = new InterleavedBlockBuilder(outOfOrderFieldTypes, new BlockBuilderStatus(), NULL_BUILDER_POSITIONS_THRESHOLD);
            }
        }

        @Override
        public void start()
        {
            if (builder == null) {
                if (nullBuilder == null || (nullBuilder.getPositionCount() >= NULL_BUILDER_POSITIONS_THRESHOLD && nullBuilder.getSizeInBytes() >= NULL_BUILDER_SIZE_IN_BYTES_THRESHOLD)) {
                    nullBuilder = rowType.createBlockBuilder(new BlockBuilderStatus(), NULL_BUILDER_POSITIONS_THRESHOLD);
                }
                currentEntryBuilder = nullBuilder;
            }
            else {
                while (builder.getPositionCount() < fieldIndex) {
                    builder.appendNull();
                }
                currentEntryBuilder = builder;
            }
            if (outOfOrderBlockBuilder != null) {
                currentEntryBuilder = outOfOrderBlockBuilder;
            }
            else {
                currentEntryBuilder = currentEntryBuilder.beginBlockEntry();
            }

            for (IndexedBlockConverter converter : converters) {
                converter.beforeValue(currentEntryBuilder);
            }
        }

        @Override
        public void end()
        {
            for (BlockConverter converter : converters) {
                converter.afterValue();
            }
            if (outOfOrderBlockBuilder != null) {
                if (builder == null) {
                    currentEntryBuilder = nullBuilder;
                }
                else {
                    currentEntryBuilder = builder;
                }

                currentEntryBuilder = currentEntryBuilder.beginBlockEntry();
                List<IndexedBlockConverter> indexedConverters = new ArrayList<>(converters);
                indexedConverters.sort((IndexedBlockConverter converter1, IndexedBlockConverter converter2)->Integer.compare(converterIndexes[converter1.getFieldIndex()], converterIndexes[converter2.getFieldIndex()]));
                int positionCount = outOfOrderBlockBuilder.getPositionCount();
                for (int i = 0; i < converterIndexes.length; i++) {
                    int position = indexedConverters.get(i).getFieldIndex();
                    if (converterIndexes[position] < 0 || position >= positionCount) {
                        continue;
                    }
                    while (currentEntryBuilder.getPositionCount() < converterIndexes[position]) {
                        currentEntryBuilder.appendNull();
                    }
                    outOfOrderBlockBuilder.writePositionTo(position, currentEntryBuilder);
                    currentEntryBuilder.closeEntry();
                }
            }
            int fieldCount = rowType.getTypeParameters().size();
            while (currentEntryBuilder.getPositionCount() < fieldCount) {
                currentEntryBuilder.appendNull();
            }

            if (builder == null) {
                nullBuilder.closeEntry();
            }
            else {
                builder.closeEntry();
            }
        }

        @Override
        public void afterValue()
        {
        }

        @Override
        public Block getBlock()
        {
            checkState(builder == null && nullBuilder != null); // check that user requested a result block (builder == null), and the program followed the request (nullBuilder != null)
            return nullBuilder.getObject(nullBuilder.getPositionCount() - 1, Block.class);
        }
    }

    private static class ParquetListConverter
            extends GroupedConverter
    {
        private static final int NULL_BUILDER_POSITIONS_THRESHOLD = 100;
        private static final int NULL_BUILDER_SIZE_IN_BYTES_THRESHOLD = 32768;

        private final Type arrayType;
        private int fieldIndex;

        private final BlockConverter elementConverter;
        private BlockBuilder builder;
        private BlockBuilder nullBuilder; // used internally when builder is set to null
        private BlockBuilder currentEntryBuilder;

        public ParquetListConverter(Type prestoType, String columnName, GroupType listType, int fieldIndex, boolean useNames, TypeManager typeManager)
        {
            checkArgument(
                    listType.getFieldCount() == 1,
                    "Expected LIST column '%s' to only have one field, but has %s fields",
                    columnName,
                    listType.getFieldCount());
            checkArgument(ARRAY.equals(prestoType.getTypeSignature().getBase()));

            this.arrayType = prestoType;
            this.fieldIndex = fieldIndex;

            // The Parquet specification requires that the element value of a
            // LIST type be wrapped in an inner repeated group, like so:
            //
            // optional group listField (LIST) {
            //   repeated group list {
            //     optional int element
            //   }
            // }
            //
            // However, some parquet libraries don't follow this spec. The
            // compatibility rules used here are specified in the Parquet
            // documentation at http://git.io/vOpNz.
            parquet.schema.Type elementType = listType.getType(0);
            if (isElementType(elementType, listType.getName())) {
                elementConverter = createConverter(prestoType.getTypeParameters().get(0), columnName + ".element", elementType, 0, useNames, typeManager);
            }
            else {
                elementConverter = new ParquetListEntryConverter(prestoType.getTypeParameters().get(0), columnName, elementType.asGroupType(), useNames, typeManager);
            }
        }

        //copied over from Apache Hive
        private boolean isElementType(parquet.schema.Type repeatedType, String parentName)
        {
            if (repeatedType.isPrimitive() ||
                    (repeatedType.asGroupType().getFieldCount() > 1)) {
                return true;
            }

            if (repeatedType.getName().equals("array")) {
                return true; // existing avro data
            }

            if (repeatedType.getName().equals(parentName + "_tuple")) {
                return true; // existing thrift data
            }
            // false for the following cases:
            // * name is "list", which matches the spec
            // * name is "bag", which indicates existing hive or pig data
            // * ambiguous case, which should be assumed is 3-level according to spec
            return false;
        }

        @Override
        public Type getType()
        {
            return arrayType;
        }

        @Override
        public void setFieldIndex(int fieldIndex)
        {
            this.fieldIndex = fieldIndex;
        }

        @Override
        public int getFieldIndex()
        {
            return fieldIndex;
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
            this.builder = builder;
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex == 0) {
                return (Converter) elementConverter;
            }
            throw new IllegalArgumentException("LIST field must be 0 not " + fieldIndex);
        }

        @Override
        public void start()
        {
            if (builder == null) {
                if (nullBuilder == null || (nullBuilder.getPositionCount() >= NULL_BUILDER_POSITIONS_THRESHOLD && nullBuilder.getSizeInBytes() >= NULL_BUILDER_SIZE_IN_BYTES_THRESHOLD)) {
                    nullBuilder = arrayType.createBlockBuilder(new BlockBuilderStatus(), NULL_BUILDER_POSITIONS_THRESHOLD);
                }
                currentEntryBuilder = nullBuilder.beginBlockEntry();
            }
            else {
                while (builder.getPositionCount() < fieldIndex) {
                    builder.appendNull();
                }
                currentEntryBuilder = builder.beginBlockEntry();
            }
            elementConverter.beforeValue(currentEntryBuilder);
        }

        @Override
        public void end()
        {
            elementConverter.afterValue();

            if (builder == null) {
                nullBuilder.closeEntry();
            }
            else {
                builder.closeEntry();
            }
        }

        @Override
        public void afterValue()
        {
        }

        @Override
        public Block getBlock()
        {
            checkState(builder == null && nullBuilder != null); // check that user requested a result block (builder == null), and the program followed the request (nullBuilder != null)
            return nullBuilder.getObject(nullBuilder.getPositionCount() - 1, Block.class);
        }
    }

    private static class ParquetListEntryConverter
            extends GroupConverter
            implements BlockConverter
    {
        private final BlockConverter elementConverter;

        private BlockBuilder builder;
        private int startingPosition;

        public ParquetListEntryConverter(Type prestoType, String columnName, GroupType elementType, boolean useNames, TypeManager typeManager)
        {
            checkArgument(
                    elementType.getOriginalType() == null,
                    "Expected LIST column '%s' field to be type STRUCT, but is %s",
                    columnName,
                    elementType);

            checkArgument(
                    elementType.getFieldCount() == 1,
                    "Expected LIST column '%s' element to have one field, but has %s fields",
                    columnName,
                    elementType.getFieldCount());

            elementConverter = createConverter(prestoType, columnName + ".element", elementType.getType(0), 0, useNames, typeManager);
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex == 0) {
                return (Converter) elementConverter;
            }
            throw new IllegalArgumentException("LIST entry field must be 0 not " + fieldIndex);
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
            this.builder = builder;
        }

        @Override
        public void start()
        {
            elementConverter.beforeValue(builder);
            startingPosition = builder.getPositionCount();
        }

        @Override
        public void end()
        {
            elementConverter.afterValue();
            // we have read nothing, this means there is a null element in this list
            if (builder.getPositionCount() == startingPosition) {
                builder.appendNull();
            }
        }

        @Override
        public void afterValue()
        {
        }
    }

    private static class ParquetMapConverter
            extends GroupedConverter
    {
        private static final int NULL_BUILDER_POSITIONS_THRESHOLD = 100;
        private static final int NULL_BUILDER_SIZE_IN_BYTES_THRESHOLD = 32768;

        private final Type mapType;
        private int fieldIndex;

        private final ParquetMapEntryConverter entryConverter;
        private BlockBuilder builder;
        private BlockBuilder nullBuilder; // used internally when builder is set to null
        private BlockBuilder currentEntryBuilder;

        public ParquetMapConverter(Type type, String columnName, GroupType mapType, int fieldIndex, boolean useNames, TypeManager typeManager)
        {
            checkArgument(
                    mapType.getFieldCount() == 1,
                    "Expected MAP column '%s' to only have one field, but has %s fields",
                    mapType.getName(),
                    mapType.getFieldCount());

            this.mapType = type;
            this.fieldIndex = fieldIndex;

            parquet.schema.Type entryType = mapType.getFields().get(0);

            entryConverter = new ParquetMapEntryConverter(type, columnName + ".entry", entryType.asGroupType(), useNames, typeManager);
        }

        @Override
        public Type getType()
        {
            return mapType;
        }

        @Override
        public void setFieldIndex(int fieldIndex)
        {
            this.fieldIndex = fieldIndex;
        }

        @Override
        public int getFieldIndex()
        {
            return fieldIndex;
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
            this.builder = builder;
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex == 0) {
                return entryConverter;
            }
            throw new IllegalArgumentException("Map field must be 0 not " + fieldIndex);
        }

        @Override
        public void start()
        {
            if (builder == null) {
                if (nullBuilder == null || (nullBuilder.getPositionCount() >= NULL_BUILDER_POSITIONS_THRESHOLD && nullBuilder.getSizeInBytes() >= NULL_BUILDER_SIZE_IN_BYTES_THRESHOLD)) {
                    nullBuilder = mapType.createBlockBuilder(new BlockBuilderStatus(), NULL_BUILDER_POSITIONS_THRESHOLD);
                }
                currentEntryBuilder = nullBuilder.beginBlockEntry();
            }
            else {
                while (builder.getPositionCount() < fieldIndex) {
                    builder.appendNull();
                }
                currentEntryBuilder = builder.beginBlockEntry();
            }
            entryConverter.beforeValue(currentEntryBuilder);
        }

        @Override
        public void end()
        {
            entryConverter.afterValue();

            if (builder == null) {
                nullBuilder.closeEntry();
            }
            else {
                builder.closeEntry();
            }
        }

        @Override
        public void afterValue()
        {
        }

        @Override
        public Block getBlock()
        {
            checkState(builder == null && nullBuilder != null); // check that user requested a result block (builder == null), and the program followed the request (nullBuilder != null)
            return nullBuilder.getObject(nullBuilder.getPositionCount() - 1, Block.class);
        }
    }

    private static class ParquetMapEntryConverter
            extends GroupConverter
            implements BlockConverter
    {
        private final BlockConverter keyConverter;
        private final BlockConverter valueConverter;

        private BlockBuilder builder;

        public ParquetMapEntryConverter(Type prestoType, String columnName, GroupType entryType, boolean useNames, TypeManager typeManager)
        {
            checkArgument(MAP.equals(prestoType.getTypeSignature().getBase()));
            // original version of parquet used null for entry due to a bug
            if (entryType.getOriginalType() != null) {
                checkArgument(
                        entryType.getOriginalType() == MAP_KEY_VALUE,
                        "Expected MAP column '%s' field to be type %s, but is %s",
                        columnName,
                        MAP_KEY_VALUE,
                        entryType);
            }

            GroupType entryGroupType = entryType.asGroupType();
            checkArgument(
                    entryGroupType.getFieldCount() == 2,
                    "Expected MAP column '%s' entry to have two fields, but has %s fields",
                    columnName,
                    entryGroupType.getFieldCount());
            checkArgument(
                    entryGroupType.getFieldName(0).equals("key"),
                    "Expected MAP column '%s' entry field 0 to be named 'key', but is named %s",
                    columnName,
                    entryGroupType.getFieldName(0));
            checkArgument(
                    entryGroupType.getFieldName(1).equals("value"),
                    "Expected MAP column '%s' entry field 1 to be named 'value', but is named %s",
                    columnName,
                    entryGroupType.getFieldName(1));
            checkArgument(
                    entryGroupType.getType(0).isPrimitive(),
                    "Expected MAP column '%s' entry field 0 to be primitive, but is %s",
                    columnName,
                    entryGroupType.getType(0));

            keyConverter = createConverter(prestoType.getTypeParameters().get(0), columnName + ".key", entryGroupType.getFields().get(0), 0, useNames, typeManager);
            valueConverter = createConverter(prestoType.getTypeParameters().get(1), columnName + ".value", entryGroupType.getFields().get(1), 1, useNames, typeManager);
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex == 0) {
                return (Converter) keyConverter;
            }
            if (fieldIndex == 1) {
                return (Converter) valueConverter;
            }
            throw new IllegalArgumentException("Map entry field must be 0 or 1 not " + fieldIndex);
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
            this.builder = builder;
        }

        @Override
        public void start()
        {
            keyConverter.beforeValue(builder);
            valueConverter.beforeValue(builder);
        }

        @Override
        public void end()
        {
            keyConverter.afterValue();
            valueConverter.afterValue();
            // handle the case where we have a key, but the value is null
            // null keys are not supported anyway, so we can ignore that case here
            if (builder.getPositionCount() % 2 != 0) {
                builder.appendNull();
            }
        }

        @Override
        public void afterValue()
        {
        }
    }

    private static class ParquetPrimitiveConverter
            extends PrimitiveConverter
            implements IndexedBlockConverter
    {
        private final Type type;
        private int fieldIndex;
        private BlockBuilder builder;

        public ParquetPrimitiveConverter(Type type, int fieldIndex)
        {
            this.type = type;
            this.fieldIndex = fieldIndex;
        }

        @Override
        public Type getType()
        {
            return type;
        }

        @Override
        public void setFieldIndex(int fieldIndex)
        {
            this.fieldIndex = fieldIndex;
        }

        @Override
        public int getFieldIndex()
        {
            return fieldIndex;
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
            this.builder = requireNonNull(builder, "parent builder is null");
        }

        @Override
        public void afterValue()
        {
        }

        private void addMissingValues()
        {
            while (builder.getPositionCount() < fieldIndex) {
                builder.appendNull();
            }
        }

        @Override
        public boolean isPrimitive()
        {
            return true;
        }

        @Override
        public PrimitiveConverter asPrimitiveConverter()
        {
            return this;
        }

        @Override
        public boolean hasDictionarySupport()
        {
            return false;
        }

        @Override
        public void setDictionary(Dictionary dictionary)
        {
        }

        @Override
        public void addValueFromDictionary(int dictionaryId)
        {
        }

        @Override
        public void addBoolean(boolean value)
        {
            addMissingValues();
            type.writeBoolean(builder, value);
        }

        @Override
        public void addDouble(double value)
        {
            addMissingValues();
            type.writeDouble(builder, value);
        }

        @Override
        public void addLong(long value)
        {
            addMissingValues();
            type.writeLong(builder, value);
        }

        @Override
        public void addBinary(Binary value)
        {
            addMissingValues();
            if (type == TIMESTAMP) {
                type.writeLong(builder, ParquetTimestampUtils.getTimestampMillis(value));
            }
            else if (isVarcharType(type)) {
                type.writeSlice(builder, truncateToLength(wrappedBuffer(value.getBytes()), type));
            }
            else if (isCharType(type)) {
                type.writeSlice(builder, trimSpacesAndTruncateToLength(wrappedBuffer(value.getBytes()), type));
            }
            else {
                type.writeSlice(builder, wrappedBuffer(value.getBytes()));
            }
        }

        @Override
        public void addFloat(float value)
        {
            addMissingValues();
            type.writeLong(builder, floatToRawIntBits(value));
        }

        @Override
        public void addInt(int value)
        {
            addMissingValues();
            type.writeLong(builder, value);
        }
    }

    private static class ParquetDecimalConverter
            extends PrimitiveConverter
            implements IndexedBlockConverter
    {
        private final DecimalType decimalType;
        private int fieldIndex;
        private BlockBuilder builder;
        private boolean wroteValue;

        public ParquetDecimalConverter(DecimalType decimalType, int fieldIndex)
        {
            this.decimalType = requireNonNull(decimalType, "decimalType is null");
            this.fieldIndex = fieldIndex;
        }

        @Override
        public Type getType()
        {
            return decimalType;
        }

        @Override
        public void setFieldIndex(int fieldIndex)
        {
            this.fieldIndex = fieldIndex;
        }

        @Override
        public int getFieldIndex()
        {
            return fieldIndex;
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
            this.builder = requireNonNull(builder, "parent builder is null");
            wroteValue = false;
        }

        @Override
        public void afterValue()
        {
            if (wroteValue) {
                return;
            }

            builder.appendNull();
        }

        @Override
        public boolean isPrimitive()
        {
            return true;
        }

        @Override
        public PrimitiveConverter asPrimitiveConverter()
        {
            return this;
        }

        @Override
        public boolean hasDictionarySupport()
        {
            return false;
        }

        @Override
        public void addBinary(Binary value)
        {
            if (decimalType.isShort()) {
                decimalType.writeLong(builder, DecimalUtils.getShortDecimalValue(value.getBytes()));
            }
            else {
                BigInteger unboundedDecimal = new BigInteger(value.getBytes());
                decimalType.writeSlice(builder, Decimals.encodeUnscaledValue(unboundedDecimal));
            }
            wroteValue = true;
        }
    }

    private static class ParquetDummyPrimitiveConverter
        extends PrimitiveConverter
        implements IndexedBlockConverter
    {
        static ParquetDummyPrimitiveConverter converter = new ParquetDummyPrimitiveConverter();

        @Override
        public Type getType()
        {
            return null;
        }

        @Override
        public void setFieldIndex(int fieldIndex)
        {
        }

        @Override
        public int getFieldIndex()
        {
            return -1;
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
        }

        @Override
        public void afterValue()
        {
        }

        @Override
        public boolean isPrimitive()
        {
            return true;
        }

        @Override
        public PrimitiveConverter asPrimitiveConverter()
        {
            return this;
        }

        @Override
        public boolean hasDictionarySupport()
        {
            return false;
        }

        @Override
        public void setDictionary(Dictionary dictionary)
        {
        }

        @Override
        public void addValueFromDictionary(int dictionaryId)
        {
        }

        @Override
        public void addBoolean(boolean value)
        {
        }

        @Override
        public void addDouble(double value)
        {
        }

        @Override
        public void addLong(long value)
        {
        }

        @Override
        public void addBinary(Binary value)
        {
        }

        @Override
        public void addFloat(float value)
        {
        }

        @Override
        public void addInt(int value)
        {
        }
    }

    private static class ParquetDummyGroupConverter
        extends GroupConverter
        implements IndexedBlockConverter
    {
        private final List<Converter> converters;

        public ParquetDummyGroupConverter(parquet.schema.Type fieldType)
        {
            requireNonNull(fieldType, "fieldType is null");
            ImmutableList.Builder<Converter> builder = ImmutableList.builder();
            for (parquet.schema.Type type : fieldType.asGroupType().getFields()) {
               builder.add(type.isPrimitive() ? ParquetDummyPrimitiveConverter.converter : new ParquetDummyGroupConverter(type));
            }
            converters = builder.build();
        }

        @Override
        public void beforeValue(BlockBuilder builder)
        {
        }

        @Override
        public void afterValue()
        {
        }

        @Override
        public void setFieldIndex(int fieldIndex)
        {
        }

        @Override
        public int getFieldIndex()
        {
            return -1;
        }

        @Override
        public Type getType()
        {
            return null;
        }

        @Override
        public void end()
        {
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return converters.get(fieldIndex);
        }

        @Override
        public void start()
        {
        }
    }
}
