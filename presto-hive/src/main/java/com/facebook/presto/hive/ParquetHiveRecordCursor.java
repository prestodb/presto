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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Charsets;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.joda.time.DateTimeZone;
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
import parquet.schema.MessageType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;

import io.airlift.log.Logger;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Map;
import java.util.Properties;

import static com.facebook.presto.hive.HiveBooleanParser.isFalse;
import static com.facebook.presto.hive.HiveBooleanParser.isTrue;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.NumberParser.parseDouble;
import static com.facebook.presto.hive.NumberParser.parseLong;
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

class ParquetHiveRecordCursor
        extends HiveRecordCursor
{
    private static final Logger log = Logger.get(ParquetHiveRecordCursor.class);

    public static final String PARQUET_COLUMN_INDEX_ACCESS = "presto.parquet.column.index.access";
    public static final String PARQUET_STRICT_TYPE_CHECKING = "presto.parquet.strict.typing";

    private final ParquetRecordReader<Void> recordReader;
    private final DateTimeZone sessionTimeZone;

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

    private final boolean strictTypeChecking;

    public ParquetHiveRecordCursor(
            Configuration configuration,
            Path path,
            long start,
            long length,
            Properties splitSchema,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> columns,
            DateTimeZone sessionTimeZone)
    {
        checkNotNull(configuration, "jobConf is null");
        checkNotNull(path, "path is null");
        checkArgument(length >= 0, "totalBytes is negative");
        checkNotNull(splitSchema, "splitSchema is null");
        checkNotNull(partitionKeys, "partitionKeys is null");
        checkNotNull(columns, "columns is null");
        checkArgument(!columns.isEmpty(), "columns is empty");
        checkNotNull(sessionTimeZone, "sessionTimeZone is null");

        this.recordReader = createParquetRecordReader(configuration, path, start, length, columns);

        this.totalBytes = length;
        this.sessionTimeZone = sessionTimeZone;

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

        HiveClientConfig defaults = new HiveClientConfig();
        this.strictTypeChecking = configuration.getBoolean(PARQUET_STRICT_TYPE_CHECKING, defaults.isParquetStrictTypeChecking());

        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);

            names[i] = column.getName();
            types[i] = column.getType();

            isPartitionColumn[i] = column.isPartitionKey();
            nullsRowDefault[i] = !column.isPartitionKey();
        }

        // parse requested partition columns
        Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey.nameGetter());
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            if (column.isPartitionKey()) {
                HivePartitionKey partitionKey = partitionKeysByName.get(column.getName());
                checkArgument(partitionKey != null, "Unknown partition key %s", column.getName());

                byte[] bytes = partitionKey.getValue().getBytes(Charsets.UTF_8);

                if (types[columnIndex].equals(BOOLEAN)) {
                    if (isTrue(bytes, 0, bytes.length)) {
                        booleans[columnIndex] = true;
                    }
                    else if (isFalse(bytes, 0, bytes.length)) {
                        booleans[columnIndex] = false;
                    }
                    else {
                        String valueString = new String(bytes, Charsets.UTF_8);
                        throw new IllegalArgumentException(String.format("Invalid partition value '%s' for BOOLEAN partition key %s", valueString, names[columnIndex]));
                    }
                }
                else if (types[columnIndex].equals(BIGINT)) {
                    if (bytes.length == 0) {
                        throw new IllegalArgumentException(String.format("Invalid partition value '' for BIGINT partition key %s", names[columnIndex]));
                    }
                    longs[columnIndex] = parseLong(bytes, 0, bytes.length);
                }
                else if (types[columnIndex].equals(DOUBLE)) {
                    if (bytes.length == 0) {
                        throw new IllegalArgumentException(String.format("Invalid partition value '' for DOUBLE partition key %s", names[columnIndex]));
                    }
                    doubles[columnIndex] = parseDouble(bytes, 0, bytes.length);
                }
                else if (types[columnIndex].equals(VARCHAR)) {
                    slices[columnIndex] = Slices.wrappedBuffer(bytes);
                }
                else {
                    throw new UnsupportedOperationException("Unsupported column type: " + types[columnIndex]);
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
            throw new PrestoException(HIVE_CURSOR_ERROR.toErrorCode(), e);
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
            PrestoReadSupport readSupport = new PrestoReadSupport(columns);

            ParquetMetadata parquetMetadata = ParquetFileReader.readFooter(configuration, path);
            List<BlockMetaData> blocks = parquetMetadata.getBlocks();
            FileMetaData fileMetaData = parquetMetadata.getFileMetaData();

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
            ParquetRecordReader<Void> realReader = new ParquetRecordReader<>(readSupport);
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

    public class PrestoReadSupport
            extends ReadSupport<Void>
    {
        private final List<HiveColumnHandle> columns;
        private List<Converter> converters;

        public PrestoReadSupport(List<HiveColumnHandle> columns)
        {
            this.columns = columns;
        }

        @Override
        @SuppressWarnings("deprecation")
        public ReadContext init(
                Configuration configuration,
                Map<String, String> keyValueMetaData,
                MessageType fileSchema)
        {
            HiveClientConfig defaults = new HiveClientConfig();

            ImmutableList.Builder<Converter> converters = ImmutableList.builder();
            ImmutableList.Builder<parquet.schema.Type> fields = ImmutableList.builder();
            for (int i = 0; i < columns.size(); i++) {
                HiveColumnHandle column = columns.get(i);
                if (!column.isPartitionKey()) {
                    parquet.schema.Type type =
                        configuration.getBoolean(PARQUET_COLUMN_INDEX_ACCESS, defaults.isParquetColumnIndexAccess()) ?
                            fileSchema.getType(column.getOrdinalPosition()) : fileSchema.getType(column.getName());
                    fields.add(type);
                    HiveType hiveType = column.getHiveType();
                    switch (hiveType) {
                        case BOOLEAN:
                        case BYTE:
                        case SHORT:
                        case STRING:
                        case INT:
                        case LONG:
                        case FLOAT:
                        case DOUBLE:
                            converters.add(new ParquetPrimitiveConverter(i, hiveType));
                            break;
                        case MAP:
                            converters.add(new ParquetMapConverter(i, type));
                            break;
                        case LIST:
                            converters.add(new ParquetListConverter(i, type));
                            break;
                        case STRUCT:
                            converters.add(new ParquetStructConverter(i, type));
                            break;
                        default:
                            break;
                    }
                }
            }
            this.converters = converters.build();
            MessageType requestedProjection = new MessageType(fileSchema.getName(), fields.build());
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
    }

    private class ParquetRecordConverter
            extends RecordMaterializer<Void>
    {
        private final GroupConverter groupConverter;

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

    public class ParquetGroupConverter
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

    private interface ParquetValueContainer
    {
        void put(Object value);

        void clear();

        void serializeToJson();
    }

    private class ParquetMapValueContainer
            implements ParquetValueContainer
    {
        private String key;
        private  Map<String, Object> buffer;
        private final parquet.schema.Type parquetSchema;
        private final int fieldIndex;

        private ParquetMapValueContainer(parquet.schema.Type parquetSchema, int fieldIndex)
        {
            this.parquetSchema = parquetSchema;
            this.fieldIndex = fieldIndex;
            this.buffer = new ConcurrentHashMap<String, Object>();
        }

        public void setKey(String inputKey)
        {
            this.key = new String(inputKey);
        }

        @Override
        public void put(Object value)
        {
            buffer.put(key, value);
        }

        @Override
        public void clear()
        {
            buffer.clear();
        }

        @Override
        public void serializeToJson()
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (JsonGenerator generator = new JsonFactory().createGenerator(out)) {
                generator.writeStartObject();
                for (Map.Entry<String, Object> entry : buffer.entrySet()) {
                    PrimitiveTypeName typeName = parquetSchema.asGroupType().getType(0).asGroupType().getType(1).asPrimitiveType().getPrimitiveTypeName();
                    generator.writeFieldName(entry.getKey());
                    serializeObject(generator, typeName, entry.getValue());
                }
                generator.writeEndObject();
            }
            catch (IOException e) {
                log.error("Error Converting Parquet Map into Json");
                throw Throwables.propagate(e);
            }
            slices[fieldIndex] = Slices.wrappedBuffer(out.toByteArray());
            clear();
        }
    }

    private class ParquetListValueContainer
            implements ParquetValueContainer
    {
        private Queue<Object> buffer;
        private final parquet.schema.Type parquetSchema;
        private final int fieldIndex;

        private ParquetListValueContainer(parquet.schema.Type parquetSchema, int fieldIndex)
        {
            this.parquetSchema = parquetSchema;
            this.fieldIndex = fieldIndex;
            this.buffer = new ConcurrentLinkedQueue<Object>();
        }

        @Override
        public void put(Object value)
        {
            buffer.add(value);
        }

        @Override
        public void clear()
        {
            buffer.clear();
        }

        @Override
        public void serializeToJson()
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (JsonGenerator generator = new JsonFactory().createGenerator(out)) {
                generator.writeStartArray();
                Iterator<Object> itr = buffer.iterator();
                while (itr.hasNext()) {
                    PrimitiveTypeName parquetTypeName =
                        parquetSchema.asGroupType().getType(0).asGroupType().getType(0).asPrimitiveType().getPrimitiveTypeName();
                    Object element = itr.next();
                    serializeObject(generator, parquetTypeName, element);
                }
                generator.writeEndArray();
            }
            catch (IOException e) {
                log.error("Error Converting Parquet Array into Json");
                throw Throwables.propagate(e);
            }
            slices[fieldIndex] = Slices.wrappedBuffer(out.toByteArray());
            clear();
        }
    }

    private class ParquetStructValueContainer
            implements ParquetValueContainer
    {
        private Map<String, Object> buffer;
        private final int fieldIndex;
        private final parquet.schema.Type parquetSchema;

        private ParquetStructValueContainer(parquet.schema.Type parquetSchema, int fieldIndex)
        {
            this.parquetSchema = parquetSchema;
            this.fieldIndex = fieldIndex;
            this.buffer = new ConcurrentHashMap<String, Object>();
        }

        @Override
        public void put(Object value)
        {
            throw new UnsupportedOperationException("Could not put a single value into a map");
        }

        public void put(String name, Object value)
        {
            buffer.put(name, value);
        }

        @Override
        public void clear()
        {
            buffer.clear();
        }

        @Override
        public void serializeToJson()
        {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            try (JsonGenerator generator = new JsonFactory().createGenerator(out)) {
                generator.writeStartObject();
                int i = 0;
                for (Map.Entry<String, Object> entry : buffer.entrySet()) {
                    String name = entry.getKey();
                    PrimitiveTypeName typeName = parquetSchema.asGroupType().getType(i).asPrimitiveType().getPrimitiveTypeName();
                    generator.writeFieldName(name);
                    serializeObject(generator, typeName, entry.getValue());
                    i = i + 1;
                }
                generator.writeEndObject();
            }
            catch (IOException e) {
                log.error("Error Converting Parquet Struct into Json");
                throw Throwables.propagate(e);
            }
            slices[fieldIndex] = Slices.wrappedBuffer(out.toByteArray());
            clear();
        }
    }

    private void serializeObject(JsonGenerator generator, PrimitiveTypeName parquetTypeName, Object element)
        throws IOException
    {
        switch (parquetTypeName) {
            case BOOLEAN:
                generator.writeBoolean((Boolean) element);
                break;
            case INT32:
            case INT64:
                generator.writeNumber((long) element);
                break;
            case FLOAT:
            case DOUBLE:
                generator.writeNumber((double) element);
                break;
            case BINARY:
                generator.writeString(((Binary) element).toStringUsingUTF8());
                break;
            case INT96:
            case FIXED_LEN_BYTE_ARRAY:
            default:
                throw new IOException("Invalid Parquet Primitive Type");
        }
    }

    public class ParquetStructConverter
        extends GroupConverter
    {
        private final int fieldIndex;
        private final List<Converter> converterList = new ArrayList<Converter>();
        private final parquet.schema.Type parquetSchema;
        private final ParquetValueContainer valueContainer;

        private ParquetStructConverter(int fieldIndex, parquet.schema.Type parquetSchema)
        {
            this.fieldIndex = fieldIndex;
            this.parquetSchema = parquetSchema;
            this.valueContainer = new ParquetStructValueContainer(parquetSchema, fieldIndex);
            for (int i = 0; i < parquetSchema.asGroupType().getFieldCount(); i++) {
                if (parquetSchema.asGroupType().getType(i).isPrimitive()) {
                    converterList.add(new ParquetStructPrimitiveConverter(fieldIndex,
                                                            parquetSchema.asGroupType().getType(i).getName(),
                                                            valueContainer));
                }
                else {
                    throw new IllegalArgumentException("Not Support Nested Struct in Parquet: " + parquetSchema.asGroupType().getType(i));
                }
            }
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            return converterList.get(fieldIndex);
        }

        @Override
        public void start()
        {
            valueContainer.clear();
        }

        @Override
        public void end()
        {
            valueContainer.serializeToJson();
        }
    }

    public class ParquetListConverter
        extends GroupConverter
    {
        private final int fieldIndex;
        private final ParquetListGroupConverter listGroupConverter;
        private final parquet.schema.Type parquetSchema;
        private final ParquetValueContainer valueContainer;

        private ParquetListConverter(int fieldIndex, parquet.schema.Type parquetSchema)
        {
            if (parquetSchema.asGroupType().getFieldCount() != 1) {
                throw new IllegalArgumentException("Invalid Parquet Array Schema: " + parquetSchema);
            }
            this.fieldIndex = fieldIndex;
            this.parquetSchema = parquetSchema;
            this.valueContainer = new ParquetListValueContainer(parquetSchema, fieldIndex);
            this.listGroupConverter = new ParquetListGroupConverter(fieldIndex,
                                                    parquetSchema.asGroupType().getType(0).asGroupType(),
                                                    valueContainer);
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex != 0) {
                throw new IllegalArgumentException("Parquet Array could not reach index: " + fieldIndex);
            }
            return listGroupConverter;
        }

        @Override
        public void start()
        {
            valueContainer.clear();
        }

        @Override
        public void end()
        {
            valueContainer.serializeToJson();
        }
    }

    public class ParquetListGroupConverter
        extends GroupConverter
    {
        private final int fieldIndex;
        private final Converter elementConverter;
        private final ParquetValueContainer valueContainer;

        private ParquetListGroupConverter(int fieldIndex,
                                        parquet.schema.GroupType parquetSchema,
                                        ParquetValueContainer valueContainer)
        {
            if (parquetSchema.getFieldCount() != 1) {
                throw new IllegalArgumentException("Invalid Parquet Array Schema: " + parquetSchema.toString());
            }
            this.fieldIndex = fieldIndex;
            this.valueContainer = valueContainer;

            if (parquetSchema.getType(0).isPrimitive()) {
                this.elementConverter = new ParquetListPrimitiveConverter(fieldIndex,
                                                                            valueContainer);
            }
            else {
                throw new IllegalArgumentException("Not Support Nested Array in Parquet");
            }
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex == 0) {
                return elementConverter;
            }
            throw new IllegalArgumentException("Parquet Array could not reach index: " + fieldIndex);
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

    public class ParquetMapConverter
        extends GroupConverter
    {
        private final int fieldIndex;
        private final ParquetMapGroupConverter mapGroupConverter;
        private final parquet.schema.Type parquetSchema;
        private final ParquetValueContainer valueContainer;

        private ParquetMapConverter(int fieldIndex, parquet.schema.Type parquetSchema)
        {
            if (parquetSchema.asGroupType().getFieldCount() != 1) {
                throw new IllegalArgumentException("Invalid Parquet Map Schema: " + parquetSchema);
            }
            this.fieldIndex = fieldIndex;
            this.parquetSchema = parquetSchema;
            this.valueContainer = new ParquetMapValueContainer(parquetSchema, fieldIndex);
            this.mapGroupConverter =
                    new ParquetMapGroupConverter(fieldIndex,
                                                parquetSchema.asGroupType().getType(0).asGroupType(),
                                                valueContainer);
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex != 0) {
                throw new IllegalArgumentException("Parquet Map could not reach index: " + fieldIndex);
            }
            return mapGroupConverter;
        }

        @Override
        public void start()
        {
            valueContainer.clear();
        }

        @Override
        public void end()
        {
            valueContainer.serializeToJson();
        }
    }

    public class ParquetMapGroupConverter
        extends GroupConverter
    {
        private final int fieldIndex;
        private final Converter keyConverter;
        private final Converter valueConverter;
        private ParquetValueContainer valueContainer;

        private ParquetMapGroupConverter(int fieldIndex,
                                        parquet.schema.GroupType parquetSchema,
                                        ParquetValueContainer valueContainer)
        {
            if (parquetSchema.getFieldCount() != 2
                || !parquetSchema.getType(0).getName().equals("key")
                || !parquetSchema.getType(1).getName().equals("value")) {
                    throw new IllegalArgumentException("Invalid Parquet Map Schema: " + parquetSchema.toString());
            }
            this.fieldIndex = fieldIndex;
            this.valueContainer = valueContainer;

            if (parquetSchema.getType(0).isPrimitive()) {
                this.keyConverter = new ParquetMapPrimitiveConverter(fieldIndex,
                                                                        true,
                                                                        valueContainer);
            }
            else {
                throw new IllegalArgumentException("Nested maps are not supported in Parquet");
            }

            if (parquetSchema.getType(1).isPrimitive()) {
                this.valueConverter = new ParquetMapPrimitiveConverter(fieldIndex,
                                                                        false,
                                                                        valueContainer);
            }
            else {
                throw new IllegalArgumentException("Nested maps are not supported in Parquet");
            }
        }

        @Override
        public Converter getConverter(int fieldIndex)
        {
            if (fieldIndex == 0) {
                return keyConverter;
            }
            else if (fieldIndex == 1) {
                return valueConverter;
            }
            throw new IllegalArgumentException("Parquet Map could not reach index: " + fieldIndex);
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

    private class ParquetMapPrimitiveConverter
            extends PrimitiveConverter
    {
        private final int fieldIndex;
        private final boolean isKey;
        private final ParquetValueContainer valueContainer;

        private ParquetMapPrimitiveConverter(int fieldIndex, boolean isKey,
                                            ParquetValueContainer valueContainer)
        {
            this.fieldIndex = fieldIndex;
            this.isKey = isKey;
            this.valueContainer = valueContainer;
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
            if (this.isKey) {
                ((ParquetMapValueContainer) valueContainer).setKey(String.valueOf(value));
            }
            else {
                valueContainer.put(Boolean.valueOf(value));
            }
        }

        @Override
        public void addDouble(double value)
        {
            nulls[fieldIndex] = false;
            if (this.isKey) {
                ((ParquetMapValueContainer) valueContainer).setKey(String.valueOf(value));
            }
            else {
                valueContainer.put(Double.valueOf(value));
            }
        }

        @Override
        public void addLong(long value)
        {
            nulls[fieldIndex] = false;
            if (this.isKey) {
                ((ParquetMapValueContainer) valueContainer).setKey(String.valueOf(value));
            }
            else {
                valueContainer.put(Long.valueOf(value));
            }
        }

        @Override
        public void addBinary(Binary value)
        {
            nulls[fieldIndex] = false;
            if (this.isKey) {
                ((ParquetMapValueContainer) valueContainer).setKey(value.toStringUsingUTF8());
            }
            else {
                valueContainer.put(value);
            }
        }

        @Override
        public void addFloat(float value)
        {
            nulls[fieldIndex] = false;
            if (this.isKey) {
                ((ParquetMapValueContainer) valueContainer).setKey(String.valueOf(value));
            }
            else {
                valueContainer.put(Double.valueOf(value));
            }
        }

        @Override
        public void addInt(int value)
        {
            nulls[fieldIndex] = false;
            if (this.isKey) {
                ((ParquetMapValueContainer) valueContainer).setKey(String.valueOf(value));
            }
            else {
                valueContainer.put(Long.valueOf(value));
            }
        }
    }

    private class ParquetListPrimitiveConverter
            extends PrimitiveConverter
    {
        private final int fieldIndex;
        private final ParquetValueContainer valueContainer;

        private ParquetListPrimitiveConverter(int fieldIndex,
                                            ParquetValueContainer valueContainer)
        {
            this.fieldIndex = fieldIndex;
            this.valueContainer = valueContainer;
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
            valueContainer.put(Boolean.valueOf(value));
        }

        @Override
        public void addDouble(double value)
        {
            nulls[fieldIndex] = false;
            valueContainer.put(Double.valueOf(value));
        }

        @Override
        public void addLong(long value)
        {
            nulls[fieldIndex] = false;
            valueContainer.put(Long.valueOf(value));
        }

        @Override
        public void addBinary(Binary value)
        {
            nulls[fieldIndex] = false;
            valueContainer.put(value);
        }

        @Override
        public void addFloat(float value)
        {
            nulls[fieldIndex] = false;
            valueContainer.put(Double.valueOf(value));
        }

        @Override
        public void addInt(int value)
        {
            nulls[fieldIndex] = false;
            valueContainer.put(Long.valueOf(value));
        }
    }

    private class ParquetStructPrimitiveConverter
            extends PrimitiveConverter
    {
        private final int fieldIndex;
        private final String name;
        private final ParquetValueContainer valueContainer;

        private ParquetStructPrimitiveConverter(int fieldIndex,
                                                String name,
                                                ParquetValueContainer valueContainer)
        {
            this.fieldIndex = fieldIndex;
            this.name = name;
            this.valueContainer = valueContainer;
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
            ((ParquetStructValueContainer) valueContainer).put(name, Boolean.valueOf(value));
        }

        @Override
        public void addDouble(double value)
        {
            nulls[fieldIndex] = false;
            ((ParquetStructValueContainer) valueContainer).put(name, Double.valueOf(value));
        }

        @Override
        public void addLong(long value)
        {
            nulls[fieldIndex] = false;
            ((ParquetStructValueContainer) valueContainer).put(name, Long.valueOf(value));
        }

        @Override
        public void addBinary(Binary value)
        {
            nulls[fieldIndex] = false;
            ((ParquetStructValueContainer) valueContainer).put(name, value);
        }

        @Override
        public void addFloat(float value)
        {
            nulls[fieldIndex] = false;
            ((ParquetStructValueContainer) valueContainer).put(name, Double.valueOf(value));
        }

        @Override
        public void addInt(int value)
        {
            nulls[fieldIndex] = false;
            ((ParquetStructValueContainer) valueContainer).put(name, Long.valueOf(value));
        }
    }

    @SuppressWarnings("AccessingNonPublicFieldOfAnotherObject")
    private class ParquetPrimitiveConverter
            extends PrimitiveConverter
    {
        private final int fieldIndex;
        private final HiveType requestType;

        private ParquetPrimitiveConverter(int fieldIndex, HiveType requestType)
        {
            this.fieldIndex = fieldIndex;
            this.requestType = requestType;
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
            if (strictTypeChecking
                && requestType != HiveType.BOOLEAN) {
                throw new UnsupportedOperationException("Strict Type Checking for Parquet, Could not convert Boolean to " + requestType);
            }

            nulls[fieldIndex] = false;
            switch (requestType) {
                case BOOLEAN:
                    booleans[fieldIndex] = value;
                    break;
                case DOUBLE:
                case FLOAT:
                    doubles[fieldIndex] = (value ? 1.0d : 0.0d);
                    break;
                case INT:
                case LONG:
                    longs[fieldIndex] = (value ? 1L : 0L);
                    break;
                case STRING:
                    slices[fieldIndex] = Slices.utf8Slice(Boolean.toString(value));
                    break;
                case MAP:
                case LIST:
                case STRUCT:
                default:
                    throw new UnsupportedOperationException("Could not convert Boolean to " + requestType);
            }
        }

        @Override
        public void addDouble(double value)
        {
            if (strictTypeChecking
                && requestType != HiveType.DOUBLE
                && requestType != HiveType.FLOAT) {
                throw new UnsupportedOperationException("Strict Type Checking for Parquet, Could not convert Double to " + requestType);
            }

            nulls[fieldIndex] = false;
            switch (requestType) {
                case BOOLEAN:
                    booleans[fieldIndex] = (value != 0);
                    break;
                case DOUBLE:
                case FLOAT:
                    doubles[fieldIndex] = value;
                    break;
                case INT:
                case LONG:
                    longs[fieldIndex] = (long) value;
                    break;
                case STRING:
                    slices[fieldIndex] = Slices.utf8Slice(Double.toString(value));
                    break;
                case MAP:
                case LIST:
                case STRUCT:
                default:
                    throw new UnsupportedOperationException("Could not convert Double to " + requestType);
            }
        }

        @Override
        public void addLong(long value)
        {
            if (strictTypeChecking
                && requestType != HiveType.LONG
                && requestType != HiveType.INT) {
                throw new UnsupportedOperationException("Strict Type Checking for Parquet, Could not convert Long to " + requestType);
            }

            nulls[fieldIndex] = false;
            switch (requestType) {
                case BOOLEAN:
                    booleans[fieldIndex] = (value != 0);
                    break;
                case DOUBLE:
                case FLOAT:
                    doubles[fieldIndex] = (double) value;
                    break;
                case INT:
                case LONG:
                    longs[fieldIndex] = value;
                    break;
                case STRING:
                    slices[fieldIndex] = Slices.utf8Slice(Long.toString(value));
                    break;
                case MAP:
                case LIST:
                case STRUCT:
                default:
                    throw new UnsupportedOperationException("Could not convert Long to " + requestType);
            }
        }

        @Override
        public void addBinary(Binary value)
        {
            if (strictTypeChecking
                && requestType != HiveType.STRING) {
                throw new UnsupportedOperationException("Strict Type Checking for Parquet, Could not convert String to " + requestType);
            }

            nulls[fieldIndex] = false;
            switch (requestType) {
                case BOOLEAN:
                    booleans[fieldIndex] = Boolean.parseBoolean(value.toStringUsingUTF8());
                    break;
                case DOUBLE:
                case FLOAT:
                    doubles[fieldIndex] = Double.parseDouble(value.toStringUsingUTF8());
                    break;
                case INT:
                case LONG:
                    longs[fieldIndex] = Long.parseLong(value.toStringUsingUTF8());
                    break;
                case STRING:
                    slices[fieldIndex] = Slices.wrappedBuffer(value.getBytes());
                    break;
                case MAP:
                case LIST:
                case STRUCT:
                default:
                    throw new UnsupportedOperationException("Could not convert Binary to " + requestType);
            }
        }

        @Override
        public void addFloat(float value)
        {
            if (strictTypeChecking
                && requestType != HiveType.DOUBLE
                && requestType != HiveType.FLOAT) {
                throw new UnsupportedOperationException("Strict Type Checking for Parquet, Could not convert Float to " + requestType);
            }

            nulls[fieldIndex] = false;
            switch (requestType) {
                case BOOLEAN:
                    booleans[fieldIndex] = (value != 0);
                    break;
                case DOUBLE:
                case FLOAT:
                    doubles[fieldIndex] = value;
                    break;
                case INT:
                case LONG:
                    longs[fieldIndex] = (long) value;
                    break;
                case STRING:
                    slices[fieldIndex] = Slices.utf8Slice(Double.toString(value));
                    break;
                case MAP:
                case LIST:
                case STRUCT:
                default:
                    throw new UnsupportedOperationException("Could not convert Float to " + requestType);
            }
        }

        @Override
        public void addInt(int value)
        {
            if (strictTypeChecking
                && requestType != HiveType.LONG
                && requestType != HiveType.INT) {
                throw new UnsupportedOperationException("Strict Type Checking for Parquet, Could not convert Int to " + requestType);
            }

            nulls[fieldIndex] = false;
            switch (requestType) {
                case BOOLEAN:
                    booleans[fieldIndex] = (value != 0);
                    break;
                case DOUBLE:
                case FLOAT:
                    doubles[fieldIndex] = (double) value;
                    break;
                case INT:
                case LONG:
                    longs[fieldIndex] = value;
                    break;
                case STRING:
                    slices[fieldIndex] = Slices.utf8Slice(Long.toString(value));
                    break;
                case MAP:
                case LIST:
                case STRUCT:
                default:
                    throw new UnsupportedOperationException("Could not convert Int to " + requestType);
            }
        }
    }
}
