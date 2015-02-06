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
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import parquet.column.Dictionary;
import parquet.hadoop.ParquetFileReader;
import parquet.hadoop.ParquetInputSplit;
import parquet.hadoop.ParquetRecordReader;
import parquet.hadoop.api.ReadSupport;
import parquet.hadoop.api.ReadSupport.ReadContext;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.FileMetaData;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.hadoop.util.ContextUtil;
import parquet.io.api.Binary;
import parquet.io.api.Converter;
import parquet.io.api.GroupConverter;
import parquet.io.api.PrimitiveConverter;
import parquet.io.api.RecordMaterializer;
import parquet.schema.GroupType;
import parquet.schema.MessageType;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.HiveUtil.bigintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.booleanPartitionKey;
import static com.facebook.presto.hive.HiveUtil.doublePartitionKey;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static parquet.schema.OriginalType.LIST;
import static parquet.schema.OriginalType.MAP;
import static parquet.schema.OriginalType.MAP_KEY_VALUE;

class ParquetHiveRecordCursor
        extends HiveRecordCursor
{
    private final ParquetRecordReader<Void> recordReader;

    @SuppressWarnings("FieldCanBeLocal") // include names for debugging
    private final String[] names;
    private final Type[] types;

    private final boolean[] isPartitionColumn;

    private final boolean[] booleans;
    private final long[] longs;
    private final double[] doubles;
    private final Slice[] slices;
    private final boolean[] nulls;
    private final boolean[] nullsRowDefault;

    private final long totalBytes;
    private long completedBytes;
    private boolean closed;

    public ParquetHiveRecordCursor(
            Configuration configuration,
            Path path,
            long start,
            long length,
            Properties splitSchema,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> columns,
            TypeManager typeManager)
    {
        checkNotNull(configuration, "jobConf is null");
        checkNotNull(path, "path is null");
        checkArgument(length >= 0, "totalBytes is negative");
        checkNotNull(splitSchema, "splitSchema is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(columns, "columns is null");

        this.recordReader = createParquetRecordReader(configuration, path, start, length, columns);

        this.totalBytes = length;

        int size = columns.size();

        this.names = new String[size];
        this.types = new Type[size];

        this.isPartitionColumn = new boolean[size];

        this.booleans = new boolean[size];
        this.longs = new long[size];
        this.doubles = new double[size];
        this.slices = new Slice[size];
        this.nulls = new boolean[size];
        this.nullsRowDefault = new boolean[size];

        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);

            names[i] = column.getName();
            types[i] = typeManager.getType(column.getTypeSignature());

            isPartitionColumn[i] = column.isPartitionKey();
            nullsRowDefault[i] = !column.isPartitionKey();
        }

        // parse requested partition columns
        Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey::getName);
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            if (column.isPartitionKey()) {
                HivePartitionKey partitionKey = partitionKeysByName.get(column.getName());
                checkArgument(partitionKey != null, "Unknown partition key %s", column.getName());

                byte[] bytes = partitionKey.getValue().getBytes(UTF_8);

                String name = names[columnIndex];
                Type type = types[columnIndex];
                if (HiveUtil.isHiveNull(bytes)) {
                    nullsRowDefault[columnIndex] = true;
                }
                else if (type.equals(BOOLEAN)) {
                    booleans[columnIndex] = booleanPartitionKey(partitionKey.getValue(), name);
                }
                else if (type.equals(BIGINT)) {
                    longs[columnIndex] = bigintPartitionKey(partitionKey.getValue(), name);
                }
                else if (type.equals(DOUBLE)) {
                    doubles[columnIndex] = doublePartitionKey(partitionKey.getValue(), name);
                }
                else if (type.equals(VARCHAR)) {
                    slices[columnIndex] = Slices.wrappedBuffer(bytes);
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for partition key: %s", type.getDisplayName(), name));
                }
            }
        }
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
            System.arraycopy(nullsRowDefault, 0, nulls, 0, isPartitionColumn.length);

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

            closeWithSuppression(e);
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
    public boolean isNull(int fieldId)
    {
        checkState(!closed, "Cursor is closed");
        return nulls[fieldId];
    }

    private void validateType(int fieldId, Class<?> javaType)
    {
        if (types[fieldId].getJavaType() != javaType) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(String.format("Expected field to be %s, actual %s (field %s)", javaType.getName(), types[fieldId].getJavaType().getName(), fieldId));
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

    private ParquetRecordReader<Void> createParquetRecordReader(Configuration configuration, Path path, long start, long length, List<HiveColumnHandle> columns)
    {
        try {
            ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(configuration, path);
            List<BlockMetaData> blocks = parquetMetadata.getBlocks();
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();

            PrestoReadSupport readSupport = new PrestoReadSupport(columns, parquetMetadata.getFileMetaData().getSchema());
            ReadContext readContext = readSupport.init(configuration, fileMetaData.getKeyValueMetaData(), fileMetaData.getSchema());

            List<BlockMetaData> splitGroup = new ArrayList<>();
            long splitStart = start;
            long splitLength = length;
            for (BlockMetaData block : blocks) {
                long firstDataPage = block.getColumns().get(0).getFirstDataPageOffset();
                if (firstDataPage >= splitStart && firstDataPage < splitStart + splitLength) {
                    splitGroup.add(block);
                }
            }

            ParquetInputSplit split;

            split = new ParquetInputSplit(path,
                    splitStart,
                    splitLength,
                    null,
                    splitGroup,
                    readContext.getRequestedSchema().toString(),
                    fileMetaData.getSchema().toString(),
                    fileMetaData.getKeyValueMetaData(),
                    readContext.getReadSupportMetadata());

            TaskAttemptContext taskContext = ContextUtil.newTaskAttemptContext(configuration, new TaskAttemptID());
            ParquetRecordReader<Void> realReader = new PrestoParquetRecordReader(readSupport);
            realReader.initialize(split, taskContext);
            return realReader;
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
        catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw Throwables.propagate(e);
        }
    }

    public class PrestoParquetRecordReader
            extends ParquetRecordReader<Void>
    {
        private final PrestoReadSupport readSupport;

        public PrestoParquetRecordReader(PrestoReadSupport readSupport)
        {
            super(readSupport);
            this.readSupport = readSupport;
        }

        @Override
        public void close()
                throws IOException
        {
            try {
                super.close();
            }
            finally {
                this.readSupport.close();
            }
        }
    }

    public class PrestoReadSupport
            extends ReadSupport<Void>
            implements Closeable
    {
        private final List<HiveColumnHandle> columns;
        private final List<Converter> converters;
        private final List<Closeable> converterCloseables;

        public PrestoReadSupport(List<HiveColumnHandle> columns, MessageType messageType)
        {
            this.columns = columns;

            ImmutableList.Builder<Converter> converters = ImmutableList.builder();
            ImmutableList.Builder<Closeable> closeableBuilder = ImmutableList.builder();
            for (int i = 0; i < columns.size(); i++) {
                HiveColumnHandle column = columns.get(i);
                if (!column.isPartitionKey() && column.getHiveColumnIndex() < messageType.getFieldCount()) {
                    parquet.schema.Type parquetType = messageType.getFields().get(column.getHiveColumnIndex());
                    if (parquetType.isPrimitive()) {
                        converters.add(new ParquetPrimitiveColumnConverter(i));
                    }
                    else {
                        GroupType groupType = parquetType.asGroupType();
                        switch (column.getTypeSignature().getBase()) {
                            case StandardTypes.ARRAY:
                                ParquetJsonColumnConverter listConverter = new ParquetJsonColumnConverter(new ParquetListJsonConverter(groupType.getName(), null, groupType), i);
                                converters.add(listConverter);
                                closeableBuilder.add(listConverter);
                                break;
                            case StandardTypes.MAP:
                                ParquetJsonColumnConverter mapConverter = new ParquetJsonColumnConverter(new ParquetMapJsonConverter(groupType.getName(), null, groupType), i);
                                converters.add(mapConverter);
                                closeableBuilder.add(mapConverter);
                                break;
                            case StandardTypes.ROW:
                                ParquetJsonColumnConverter rowConverter = new ParquetJsonColumnConverter(new ParquetStructJsonConverter(groupType.getName(), null, groupType), i);
                                converters.add(rowConverter);
                                closeableBuilder.add(rowConverter);
                                break;
                            default:
                                throw new IllegalArgumentException("Group column " + groupType.getName() + " type " + groupType.getOriginalType() + " not supported");
                        }
                    }
                }
            }
            this.converters = converters.build();
            this.converterCloseables = closeableBuilder.build();
        }

        @Override
        @SuppressWarnings("deprecation")
        public ReadContext init(
                Configuration configuration,
                Map<String, String> keyValueMetaData,
                MessageType messageType)
        {
            ImmutableList.Builder<parquet.schema.Type> fields = ImmutableList.builder();
            for (HiveColumnHandle column : columns) {
                if (!column.isPartitionKey() && column.getHiveColumnIndex() < messageType.getFieldCount()) {
                    fields.add(messageType.getType(column.getName()));
                }
            }
            MessageType requestedProjection = new MessageType(messageType.getName(), fields.build());
            return new ReadContext(requestedProjection);
        }

        @Override
        public RecordMaterializer<Void> prepareForRead(
                Configuration configuration,
                Map<String, String> keyValueMetaData,
                MessageType fileSchema,
                ReadContext readContext)
        {
            return new ParquetRecordConverter(converters);
        }

        @Override
        public void close()
                throws IOException
        {
            for (Closeable closeable : converterCloseables) {
                closeQuietly(closeable);
            }
        }
    }

    private static class ParquetRecordConverter
            extends RecordMaterializer<Void>
    {
        private final ParquetGroupConverter groupConverter;

        public ParquetRecordConverter(List<Converter> converters)
        {
            groupConverter = new ParquetGroupConverter(converters);
        }

        @Override
        public Void getCurrentRecord()
        {
            return null;
        }

        @Override
        public GroupConverter getRootConverter()
        {
            return groupConverter;
        }
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
            slices[fieldIndex] = Slices.wrappedBuffer(value.getBytes());
        }

        @Override
        public void addFloat(float value)
        {
            nulls[fieldIndex] = false;
            doubles[fieldIndex] = value;
        }

        @Override
        public void addInt(int value)
        {
            nulls[fieldIndex] = false;
            longs[fieldIndex] = value;
        }
    }

    public class ParquetJsonColumnConverter
            extends GroupConverter
            implements Closeable
    {
        private final DynamicSliceOutput out = new DynamicSliceOutput(1024);

        private final GroupedJsonConverter jsonConverter;
        private final int fieldIndex;

        private JsonGenerator generator;

        public ParquetJsonColumnConverter(GroupedJsonConverter jsonConverter, int fieldIndex)
        {
            this.jsonConverter = jsonConverter;
            this.fieldIndex = fieldIndex;
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return jsonConverter.getConverter(fieldIndex);
        }

        @Override
        public void start()
        {
            try {
                out.reset();
                generator = new JsonFactory().createGenerator(out);
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            jsonConverter.beforeValue(generator);
            jsonConverter.start();
        }

        @Override
        public void end()
        {
            jsonConverter.end();
            jsonConverter.afterValue();

            nulls[fieldIndex] = false;
            try {
                generator.close();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
            slices[fieldIndex] = out.copySlice();
        }

        @Override
        public void close()
                throws IOException
        {
            closeQuietly(jsonConverter);
        }
    }

    private interface JsonConverter
            extends Closeable
    {
        void beforeValue(JsonGenerator generator);

        void afterValue();
    }

    private abstract static class GroupedJsonConverter
            extends GroupConverter
            implements JsonConverter
    {
    }

    private static JsonConverter createJsonConverter(String columnName, String fieldName, parquet.schema.Type type)
    {
        if (type.isPrimitive()) {
            return new ParquetPrimitiveJsonConverter(fieldName);
        }
        else if (type.getOriginalType() == LIST) {
            return new ParquetListJsonConverter(columnName, fieldName, type.asGroupType());
        }
        else if (type.getOriginalType() == MAP) {
            return new ParquetMapJsonConverter(columnName, fieldName, type.asGroupType());
        }
        else if (type.getOriginalType() == null) {
            // struct does not have an original type
            return new ParquetStructJsonConverter(columnName, fieldName, type.asGroupType());
        }
        throw new IllegalArgumentException("Unsupported type " + type);
    }

    private static class ParquetStructJsonConverter
            extends GroupedJsonConverter
    {
        private final String fieldName;
        private final List<JsonConverter> converters;
        private JsonGenerator generator;

        public ParquetStructJsonConverter(String columnName, String fieldName, GroupType entryType)
        {
            this.fieldName = fieldName;
            ImmutableList.Builder<JsonConverter> converters = ImmutableList.builder();
            for (parquet.schema.Type fieldType : entryType.getFields()) {
                converters.add(createJsonConverter(columnName + "." + fieldType.getName(), null, fieldType));
            }
            this.converters = converters.build();
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return (Converter) converters.get(fieldIndex);
        }

        @Override
        public void beforeValue(JsonGenerator generator)
        {
            this.generator = generator;
            for (JsonConverter converter : converters) {
                converter.beforeValue(generator);
            }
        }

        @Override
        public void start()
        {
            try {
                writeFieldNameIfSet(generator, fieldName);
                generator.writeStartArray();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void end()
        {
            try {
                generator.writeEndArray();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void afterValue()
        {
            for (JsonConverter converter : converters) {
                converter.afterValue();
            }
        }

        @Override
        public void close()
                throws IOException
        {
            for (JsonConverter converter : converters) {
                closeQuietly(converter);
            }
        }
    }

    private static class ParquetListJsonConverter
            extends GroupedJsonConverter
    {
        private final JsonConverter elementConverter;
        private final String fieldName;
        private JsonGenerator generator;

        public ParquetListJsonConverter(String columnName, String fieldName, GroupType listType)
        {
            this.fieldName = fieldName;

            checkArgument(listType.getFieldCount() == 1,
                    "Expected LIST column '%s' to only have one field, but has %s fields",
                    columnName,
                    listType.getFieldCount());

            elementConverter = new ParquetListEntryJsonConverter(fieldName, listType.getType(0).asGroupType());
        }

        @Override
        public void beforeValue(JsonGenerator generator)
        {
            this.generator = generator;
            elementConverter.beforeValue(generator);
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
            try {
                writeFieldNameIfSet(generator, fieldName);
                generator.writeStartArray();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void end()
        {
            try {
                generator.writeEndArray();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void afterValue()
        {
            elementConverter.afterValue();
        }

        @Override
        public void close()
                throws IOException
        {
            closeQuietly(elementConverter);
        }
    }

    private static class ParquetListEntryJsonConverter
            extends GroupConverter
            implements JsonConverter
    {
        private final JsonConverter elementConverter;

        public ParquetListEntryJsonConverter(String columnName, GroupType elementType)
        {
            checkArgument(elementType.getOriginalType() == null,
                    "Expected LIST column '%s' field to be type STRUCT, but is %s",
                    columnName,
                    elementType);

            checkArgument(elementType.getFieldCount() == 1,
                    "Expected LIST column '%s' element to have one field, but has %s fields",
                    columnName,
                    elementType.getFieldCount());

            checkArgument(elementType.getFieldName(0).equals("array_element"),
                    "Expected LIST column '%s' entry field 0 to be named 'array_element', but is named %s",
                    columnName,
                    elementType.getFieldName(0));

            elementConverter = createJsonConverter(columnName + ".element", null, elementType.getType(0));
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex == 0) {
                return (Converter) elementConverter;
            }
            throw new IllegalArgumentException("LIST entry field must be 0 or 1 not " + fieldIndex);
        }

        @Override
        public void beforeValue(JsonGenerator generator)
        {
            elementConverter.beforeValue(generator);
        }

        @Override
        public void start()
        {
        }

        @Override
        public void end()
        {
        }

        @Override
        public void afterValue()
        {
            elementConverter.afterValue();
        }

        @Override
        public void close()
                throws IOException
        {
            closeQuietly(elementConverter);
        }
    }

    private static class ParquetMapJsonConverter
            extends GroupedJsonConverter
    {
        private final ParquetMapEntryJsonConverter entryConverter;
        private final String fieldName;
        private JsonGenerator generator;

        public ParquetMapJsonConverter(String columnName, String fieldName, GroupType mapType)
        {
            this.fieldName = fieldName;

            checkArgument(mapType.getFieldCount() == 1,
                    "Expected MAP column '%s' to only have one field, but has %s fields",
                    mapType.getName(),
                    mapType.getFieldCount());

            parquet.schema.Type entryType = mapType.getFields().get(0);

            // original versions of parquet had map end entry swapped
            if (mapType.getOriginalType() != MAP_KEY_VALUE) {
                checkArgument(entryType.getOriginalType() == MAP_KEY_VALUE,
                        "Expected MAP column '%s' field to be type %s, but is %s",
                        mapType.getName(),
                        MAP_KEY_VALUE,
                        entryType);
            }

            entryConverter = new ParquetMapEntryJsonConverter(columnName + ".entry", entryType.asGroupType());
        }

        @Override
        public void beforeValue(JsonGenerator generator)
        {
            this.generator = generator;
            entryConverter.beforeValue(generator);
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
            try {
                writeFieldNameIfSet(generator, fieldName);
                generator.writeStartObject();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void end()
        {
            try {
                generator.writeEndObject();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void afterValue()
        {
            entryConverter.afterValue();
        }

        @Override
        public void close()
                throws IOException
        {
            closeQuietly(entryConverter);
        }
    }

    private static class ParquetMapEntryJsonConverter
            extends GroupConverter
            implements JsonConverter
    {
        private final JsonConverter keyConverter;
        private final JsonConverter valueConverter;

        public ParquetMapEntryJsonConverter(String columnName, GroupType entryType)
        {
            // original version of parquet used null for entry due to a bug
            if (entryType.getOriginalType() != null) {
                checkArgument(entryType.getOriginalType() == MAP_KEY_VALUE,
                        "Expected MAP column '%s' field to be type %s, but is %s",
                        columnName,
                        MAP_KEY_VALUE,
                        entryType);
            }

            GroupType entryGroupType = entryType.asGroupType();
            checkArgument(entryGroupType.getFieldCount() == 2,
                    "Expected MAP column '%s' entry to have two fields, but has %s fields",
                    columnName,
                    entryGroupType.getFieldCount());
            checkArgument(entryGroupType.getFieldName(0).equals("key"),
                    "Expected MAP column '%s' entry field 0 to be named 'key', but is named %s",
                    columnName,
                    entryGroupType.getFieldName(0));
            checkArgument(entryGroupType.getFieldName(1).equals("value"),
                    "Expected MAP column '%s' entry field 1 to be named 'value', but is named %s",
                    columnName,
                    entryGroupType.getFieldName(1));
            checkArgument(entryGroupType.getType(0).isPrimitive(),
                    "Expected MAP column '%s' entry field 0 to be primitive, but is named %s",
                    columnName,
                    entryGroupType.getType(0));

            keyConverter = new ParquetMapKeyJsonConverter();
            valueConverter = createJsonConverter(columnName + ".value", null, entryGroupType.getFields().get(1));
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
        public void beforeValue(JsonGenerator generator)
        {
            keyConverter.beforeValue(generator);
            valueConverter.beforeValue(generator);
        }

        @Override
        public void start()
        {
        }

        @Override
        public void end()
        {
        }

        @Override
        public void afterValue()
        {
            keyConverter.afterValue();
            valueConverter.afterValue();
        }

        @Override
        public void close()
                throws IOException
        {
            closeQuietly(keyConverter);
            closeQuietly(valueConverter);
        }
    }

    private static class ParquetPrimitiveJsonConverter
            extends PrimitiveConverter
            implements JsonConverter
    {
        private final String fieldName;
        private JsonGenerator generator;
        private boolean wroteValue;

        public ParquetPrimitiveJsonConverter(String fieldName)
        {
            this.fieldName = fieldName;
        }

        @Override
        public void beforeValue(JsonGenerator generator)
        {
            this.generator = generator;
            wroteValue = false;
        }

        @Override
        public void afterValue()
        {
            if (wroteValue) {
                return;
            }

            try {
                writeFieldNameIfSet(generator, fieldName);
                generator.writeNull();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
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
            try {
                writeFieldNameIfSet(generator, fieldName);
                generator.writeBoolean(value);
                wroteValue = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void addDouble(double value)
        {
            try {
                writeFieldNameIfSet(generator, fieldName);
                generator.writeNumber(value);
                wroteValue = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void addLong(long value)
        {
            try {
                writeFieldNameIfSet(generator, fieldName);
                generator.writeNumber(value);
                wroteValue = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void addBinary(Binary value)
        {
            try {
                writeFieldNameIfSet(generator, fieldName);
                // todo don't assume binary is a utf-8 string
                byte[] bytes = value.getBytes();
                generator.writeUTF8String(value.getBytes(), 0, bytes.length);
                wroteValue = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void addFloat(float value)
        {
            try {
                writeFieldNameIfSet(generator, fieldName);
                generator.writeNumber(value);
                wroteValue = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void addInt(int value)
        {
            try {
                writeFieldNameIfSet(generator, fieldName);
                generator.writeNumber(value);
                wroteValue = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void close()
                throws IOException
        {
        }
    }

    private static class ParquetMapKeyJsonConverter
            extends PrimitiveConverter
            implements JsonConverter
    {
        private JsonGenerator generator;
        private boolean wroteValue;

        @Override
        public void beforeValue(JsonGenerator generator)
        {
            this.generator = generator;
            wroteValue = false;
        }

        @Override
        public void afterValue()
        {
            checkState(wroteValue, "Null map keys are not allowed");
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
            try {
                generator.writeFieldName(String.valueOf(value));
                wroteValue = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void addDouble(double value)
        {
            try {
                generator.writeFieldName(String.valueOf(value));
                wroteValue = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void addLong(long value)
        {
            try {
                generator.writeFieldName(String.valueOf(value));
                wroteValue = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void addBinary(Binary value)
        {
            try {
                // todo don't assume binary is a utf-8 string
                generator.writeFieldName(value.toStringUsingUTF8());
                wroteValue = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void addFloat(float value)
        {
            try {
                generator.writeFieldName(String.valueOf(value));
                wroteValue = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void addInt(int value)
        {
            try {
                generator.writeFieldName(String.valueOf(value));
                wroteValue = true;
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public void close()
                throws IOException
        {
        }
    }

    private static void writeFieldNameIfSet(JsonGenerator generator, String fieldName)
            throws IOException
    {
        if (fieldName != null) {
            generator.writeFieldName(fieldName);
        }
    }

    private static void closeQuietly(Closeable closeable)
    {
        try {
            if (closeable != null) {
                closeable.close();
            }
        }
        catch (IOException ignored) {
        }
    }
}
