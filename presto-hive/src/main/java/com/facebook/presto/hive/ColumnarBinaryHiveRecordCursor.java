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
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.ByteArrays;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.columnar.BytesRefWritable;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.lazy.ByteArrayRef;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryFactory;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryObject;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapred.RecordReader;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.HiveType.HIVE_BYTE;
import static com.facebook.presto.hive.HiveType.HIVE_DATE;
import static com.facebook.presto.hive.HiveType.HIVE_DOUBLE;
import static com.facebook.presto.hive.HiveType.HIVE_FLOAT;
import static com.facebook.presto.hive.HiveType.HIVE_INT;
import static com.facebook.presto.hive.HiveType.HIVE_LONG;
import static com.facebook.presto.hive.HiveType.HIVE_SHORT;
import static com.facebook.presto.hive.HiveType.HIVE_TIMESTAMP;
import static com.facebook.presto.hive.HiveUtil.bigintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.booleanPartitionKey;
import static com.facebook.presto.hive.HiveUtil.charPartitionKey;
import static com.facebook.presto.hive.HiveUtil.datePartitionKey;
import static com.facebook.presto.hive.HiveUtil.doublePartitionKey;
import static com.facebook.presto.hive.HiveUtil.floatPartitionKey;
import static com.facebook.presto.hive.HiveUtil.getPrefilledColumnValue;
import static com.facebook.presto.hive.HiveUtil.getTableObjectInspector;
import static com.facebook.presto.hive.HiveUtil.integerPartitionKey;
import static com.facebook.presto.hive.HiveUtil.isStructuralType;
import static com.facebook.presto.hive.HiveUtil.longDecimalPartitionKey;
import static com.facebook.presto.hive.HiveUtil.shortDecimalPartitionKey;
import static com.facebook.presto.hive.HiveUtil.smallintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.timestampPartitionKey;
import static com.facebook.presto.hive.HiveUtil.tinyintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.varcharPartitionKey;
import static com.facebook.presto.hive.util.DecimalUtils.getLongDecimalValue;
import static com.facebook.presto.hive.util.DecimalUtils.getShortDecimalValue;
import static com.facebook.presto.hive.util.SerDeUtils.getBlockObject;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.Chars.trimSpacesAndTruncateToLength;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.isLongDecimal;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.StandardTypes.DECIMAL;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Maps.uniqueIndex;
import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;

class ColumnarBinaryHiveRecordCursor<K>
        extends HiveRecordCursor
{
    private final RecordReader<K, BytesRefArrayWritable> recordReader;
    private final K key;
    private final BytesRefArrayWritable value;

    @SuppressWarnings("FieldCanBeLocal") // include names for debugging
    private final String[] names;
    private final Type[] types;
    private final HiveType[] hiveTypes;

    private final ObjectInspector[] fieldInspectors; // DON'T USE THESE UNLESS EXTRACTION WILL BE SLOW ANYWAY

    private final int[] hiveColumnIndexes;

    private final boolean[] isPrefilledColumn;

    private final boolean[] loaded;
    private final boolean[] booleans;
    private final long[] longs;
    private final double[] doubles;
    private final Slice[] slices;
    private final Object[] objects;
    private final boolean[] nulls;

    private final long totalBytes;
    private long completedBytes;
    private boolean closed;

    private final HiveDecimalWritable decimalWritable = new HiveDecimalWritable();

    private static final byte HIVE_EMPTY_STRING_BYTE = (byte) 0xbf;

    private static final int SIZE_OF_SHORT = 2;
    private static final int SIZE_OF_INT = 4;
    private static final int SIZE_OF_LONG = 8;

    private static final Set<PrimitiveCategory> VALID_HIVE_STRING_TYPES = ImmutableSet.of(PrimitiveCategory.BINARY, PrimitiveCategory.VARCHAR, PrimitiveCategory.STRING);
    private static final Set<Category> VALID_HIVE_STRUCTURAL_CATEGORIES = ImmutableSet.of(Category.LIST, Category.MAP, Category.STRUCT);

    public ColumnarBinaryHiveRecordCursor(RecordReader<K, BytesRefArrayWritable> recordReader,
            long totalBytes,
            Properties splitSchema,
            List<HivePartitionKey> partitionKeys,
            List<HiveColumnHandle> columns,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            Path path)
    {
        requireNonNull(recordReader, "recordReader is null");
        checkArgument(totalBytes >= 0, "totalBytes is negative");
        requireNonNull(splitSchema, "splitSchema is null");
        requireNonNull(partitionKeys, "partitionKeys is null");
        requireNonNull(columns, "columns is null");
        requireNonNull(path, "path is null");

        this.recordReader = recordReader;
        this.totalBytes = totalBytes;
        this.key = recordReader.createKey();
        this.value = recordReader.createValue();

        int size = columns.size();

        this.names = new String[size];
        this.types = new Type[size];
        this.hiveTypes = new HiveType[size];

        this.fieldInspectors = new ObjectInspector[size];

        this.hiveColumnIndexes = new int[size];

        this.isPrefilledColumn = new boolean[size];

        this.loaded = new boolean[size];
        this.booleans = new boolean[size];
        this.longs = new long[size];
        this.doubles = new double[size];
        this.slices = new Slice[size];
        this.objects = new Object[size];
        this.nulls = new boolean[size];

        // initialize data columns
        StructObjectInspector rowInspector = getTableObjectInspector(splitSchema);

        for (int i = 0; i < columns.size(); i++) {
            HiveColumnHandle column = columns.get(i);

            names[i] = column.getName();
            types[i] = typeManager.getType(column.getTypeSignature());
            hiveTypes[i] = column.getHiveType();
            hiveColumnIndexes[i] = column.getHiveColumnIndex();
            isPrefilledColumn[i] = column.isPartitionKey() || column.isHidden();

            if (!isPrefilledColumn[i]) {
                fieldInspectors[i] = rowInspector.getStructFieldRef(column.getName()).getFieldObjectInspector();
            }
        }

        // parse requested partition columns
        Map<String, HivePartitionKey> partitionKeysByName = uniqueIndex(partitionKeys, HivePartitionKey::getName);
        for (int columnIndex = 0; columnIndex < columns.size(); columnIndex++) {
            HiveColumnHandle column = columns.get(columnIndex);
            if (isPrefilledColumn[columnIndex]) {
                HivePartitionKey partitionKey = partitionKeysByName.get(column.getName());

                String columnValue = getPrefilledColumnValue(column, partitionKey, path);
                byte[] bytes = columnValue.getBytes(UTF_8);

                String name = names[columnIndex];
                Type type = types[columnIndex];
                if (HiveUtil.isHiveNull(bytes)) {
                    nulls[columnIndex] = true;
                }
                else if (BOOLEAN.equals(type)) {
                    booleans[columnIndex] = booleanPartitionKey(columnValue, name);
                }
                else if (TINYINT.equals(type)) {
                    longs[columnIndex] = tinyintPartitionKey(columnValue, name);
                }
                else if (SMALLINT.equals(type)) {
                    longs[columnIndex] = smallintPartitionKey(columnValue, name);
                }
                else if (INTEGER.equals(type)) {
                    longs[columnIndex] = integerPartitionKey(columnValue, name);
                }
                else if (BIGINT.equals(type)) {
                    longs[columnIndex] = bigintPartitionKey(columnValue, name);
                }
                else if (REAL.equals(type)) {
                    longs[columnIndex] = floatPartitionKey(partitionKey.getValue(), name);
                }
                else if (DOUBLE.equals(type)) {
                    doubles[columnIndex] = doublePartitionKey(columnValue, name);
                }
                else if (isVarcharType(type)) {
                    slices[columnIndex] = varcharPartitionKey(columnValue, name, type);
                }
                else if (isCharType(type)) {
                    slices[columnIndex] = charPartitionKey(columnValue, name, type);
                }
                else if (DATE.equals(type)) {
                    longs[columnIndex] = datePartitionKey(columnValue, name);
                }
                else if (TIMESTAMP.equals(type)) {
                    longs[columnIndex] = timestampPartitionKey(columnValue, hiveStorageTimeZone, name);
                }
                else if (isShortDecimal(type)) {
                    longs[columnIndex] = shortDecimalPartitionKey(columnValue, (DecimalType) type, name);
                }
                else if (isLongDecimal(type)) {
                    slices[columnIndex] = longDecimalPartitionKey(columnValue, (DecimalType) type, name);
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for prefilled column: %s", type.getDisplayName(), name));
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
            if (closed || !recordReader.next(key, value)) {
                close();
                return false;
            }

            // reset loaded flags
            // prefilled values are already loaded, but everything else is not
            System.arraycopy(isPrefilledColumn, 0, loaded, 0, isPrefilledColumn.length);

            return true;
        }
        catch (IOException | RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public boolean getBoolean(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, BOOLEAN);
        if (!loaded[fieldId]) {
            parseBooleanColumn(fieldId);
        }
        return booleans[fieldId];
    }

    private void parseBooleanColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPrefilledColumn[column], "Column is a prefilled");

        loaded[column] = true;

        if (hiveColumnIndexes[column] >= value.size()) {
            // this partition may contain fewer fields than what's declared in the schema
            // this happens when additional columns are added to the hive table after a partition has been created
            nulls[column] = true;
        }
        else {
            BytesRefWritable fieldData = value.unCheckedGet(hiveColumnIndexes[column]);

            byte[] bytes;
            try {
                bytes = fieldData.getData();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            int start = fieldData.getStart();
            int length = fieldData.getLength();

            parseBooleanColumn(column, bytes, start, length);
        }
    }

    private void parseBooleanColumn(int column, byte[] bytes, int start, int length)
    {
        if (length > 0) {
            booleans[column] = bytes[start] != 0;
            nulls[column] = false;
        }
        else {
            nulls[column] = true;
        }
    }

    @Override
    public long getLong(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        if (!types[fieldId].equals(BIGINT) &&
                !types[fieldId].equals(INTEGER) &&
                !types[fieldId].equals(SMALLINT) &&
                !types[fieldId].equals(TINYINT) &&
                !types[fieldId].equals(DATE) &&
                !types[fieldId].equals(TIMESTAMP) &&
                !isShortDecimal(types[fieldId]) &&
                !types[fieldId].equals(REAL)) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(
                    format("Expected field to be %s, %s, %s, %s, %s, %s, %s or %s , actual %s (field %s)", TINYINT, SMALLINT, INTEGER, BIGINT, DATE, TIMESTAMP, DECIMAL, REAL, types[fieldId], fieldId));
        }
        if (!loaded[fieldId]) {
            parseLongColumn(fieldId);
        }
        return longs[fieldId];
    }

    private void parseLongColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPrefilledColumn[column], "Column is a prefilled");

        loaded[column] = true;

        if (hiveColumnIndexes[column] >= value.size()) {
            // this partition may contain fewer fields than what's declared in the schema
            // this happens when additional columns are added to the hive table after a partition has been created
            nulls[column] = true;
        }
        else {
            BytesRefWritable fieldData = value.unCheckedGet(hiveColumnIndexes[column]);

            byte[] bytes;
            try {
                bytes = fieldData.getData();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            int start = fieldData.getStart();
            int length = fieldData.getLength();

            parseLongColumn(column, bytes, start, length);
        }
    }

    private void parseLongColumn(int column, byte[] bytes, int start, int length)
    {
        if (length == 0) {
            nulls[column] = true;
            return;
        }
        nulls[column] = false;
        if (hiveTypes[column].equals(HIVE_SHORT)) {
            // the file format uses big endian
            checkState(length == SIZE_OF_SHORT, "Short should be 2 bytes");
            longs[column] = Short.reverseBytes(ByteArrays.getShort(bytes, start));
        }
        else if (hiveTypes[column].equals(HIVE_DATE)) {
            checkState(length >= 1, "Date should be at least 1 byte");
            long daysSinceEpoch = readVInt(bytes, start, length);
            longs[column] = daysSinceEpoch;
        }
        else if (hiveTypes[column].equals(HIVE_TIMESTAMP)) {
            checkState(length >= 1, "Timestamp should be at least 1 byte");
            long seconds = TimestampWritable.getSeconds(bytes, start);
            long nanos = (bytes[start] >> 7) != 0 ? TimestampWritable.getNanos(bytes, start + SIZE_OF_INT) : 0;
            longs[column] = (seconds * 1000) + (nanos / 1_000_000);
        }
        else if (hiveTypes[column].equals(HIVE_BYTE)) {
            checkState(length == 1, "Byte should be 1 byte");
            longs[column] = bytes[start];
        }
        else if (hiveTypes[column].equals(HIVE_INT)) {
            checkState(length >= 1, "Int should be at least 1 byte");
            if (length == 1) {
                longs[column] = bytes[start];
            }
            else {
                longs[column] = readVInt(bytes, start, length);
            }
        }
        else if (hiveTypes[column].equals(HIVE_LONG)) {
            checkState(length >= 1, "Long should be at least 1 byte");
            if (length == 1) {
                longs[column] = bytes[start];
            }
            else {
                longs[column] = readVInt(bytes, start, length);
            }
        }
        else if (hiveTypes[column].equals(HIVE_FLOAT)) {
            // the file format uses big endian
            checkState(length == SIZE_OF_INT, "Float should be 4 bytes");
            int intBits = ByteArrays.getInt(bytes, start);
            longs[column] = Integer.reverseBytes(intBits);
        }
        else {
            throw new RuntimeException(format("%s is not a valid LONG type", hiveTypes[column]));
        }
    }

    private static long readVInt(byte[] bytes, int start, int length)
    {
        long value = 0;
        for (int i = 1; i < length; i++) {
            value <<= 8;
            value |= (bytes[start + i] & 0xFF);
        }
        return WritableUtils.isNegativeVInt(bytes[start]) ? ~value : value;
    }

    @Override
    public double getDouble(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        validateType(fieldId, DOUBLE);
        if (!loaded[fieldId]) {
            parseDoubleColumn(fieldId);
        }
        return doubles[fieldId];
    }

    private void parseDoubleColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPrefilledColumn[column], "Column is a prefilled");

        loaded[column] = true;

        if (hiveColumnIndexes[column] >= value.size()) {
            // this partition may contain fewer fields than what's declared in the schema
            // this happens when additional columns are added to the hive table after a partition has been created
            nulls[column] = true;
        }
        else {
            BytesRefWritable fieldData = value.unCheckedGet(hiveColumnIndexes[column]);

            byte[] bytes;
            try {
                bytes = fieldData.getData();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            int start = fieldData.getStart();
            int length = fieldData.getLength();

            parseDoubleColumn(column, bytes, start, length);
        }
    }

    private void parseDoubleColumn(int column, byte[] bytes, int start, int length)
    {
        if (length == 0) {
            nulls[column] = true;
        }
        else {
            checkState(hiveTypes[column].equals(HIVE_DOUBLE), "%s is not a valid DOUBLE type", hiveTypes[column]);

            nulls[column] = false;
            // the file format uses big endian
            checkState(length == SIZE_OF_LONG, "Double should be 8 bytes");
            long longBits = ByteArrays.getLong(bytes, start);
            doubles[column] = Double.longBitsToDouble(Long.reverseBytes(longBits));
        }
    }

    @Override
    public Slice getSlice(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        Type type = types[fieldId];
        if (!isVarcharType(type) && !isCharType(type) && !type.equals(VARBINARY) && !isStructuralType(hiveTypes[fieldId]) && !isLongDecimal(type)) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(format("Expected field to be VARCHAR, CHAR, VARBINARY or DECIMAL, actual %s (field %s)", type, fieldId));
        }

        if (!loaded[fieldId]) {
            parseStringColumn(fieldId);
        }
        return slices[fieldId];
    }

    private void parseStringColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPrefilledColumn[column], "Column is a prefilled");

        loaded[column] = true;

        if (hiveColumnIndexes[column] >= value.size()) {
            // this partition may contain fewer fields than what's declared in the schema
            // this happens when additional columns are added to the hive table after a partition has been created
            nulls[column] = true;
        }
        else {
            BytesRefWritable fieldData = value.unCheckedGet(hiveColumnIndexes[column]);

            byte[] bytes;
            try {
                bytes = fieldData.getData();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            int start = fieldData.getStart();
            int length = fieldData.getLength();

            parseStringColumn(column, bytes, start, length);
        }
    }

    private void parseStringColumn(int column, byte[] bytes, int start, int length)
    {
        checkState(isValidHiveStringType(hiveTypes[column]), "%s is not a valid STRING type", hiveTypes[column]);
        if (length == 0) {
            nulls[column] = true;
        }
        else {
            nulls[column] = false;
            // TODO: zero length BINARY is not supported. See https://issues.apache.org/jira/browse/HIVE-2483
            if (hiveTypes[column].equals(HiveType.HIVE_STRING) && (length == 1) && bytes[start] == HIVE_EMPTY_STRING_BYTE) {
                slices[column] = Slices.EMPTY_SLICE;
            }
            else {
                Slice value = Slices.wrappedBuffer(Arrays.copyOfRange(bytes, start, start + length));
                Type type = types[column];
                if (isVarcharType(type)) {
                    slices[column] = truncateToLength(value, type);
                }
                else {
                    slices[column] = value;
                }
            }
        }
    }

    private void parseCharColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPrefilledColumn[column], "Column is a partition key");

        loaded[column] = true;

        if (hiveColumnIndexes[column] >= value.size()) {
            // this partition may contain fewer fields than what's declared in the schema
            // this happens when additional columns are added to the hive table after a partition has been created
            nulls[column] = true;
        }
        else {
            BytesRefWritable fieldData = value.unCheckedGet(hiveColumnIndexes[column]);

            byte[] bytes;
            try {
                bytes = fieldData.getData();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            int start = fieldData.getStart();
            int length = fieldData.getLength();

            parseCharColumn(column, bytes, start, length);
        }
    }

    private void parseCharColumn(int column, byte[] bytes, int start, int length)
    {
        if (length == 0) {
            nulls[column] = true;
        }
        else {
            nulls[column] = false;
            Slice value = Slices.wrappedBuffer(Arrays.copyOfRange(bytes, start, start + length));
            Type type = types[column];
            slices[column] = trimSpacesAndTruncateToLength(value, type);
        }
    }

    private void parseDecimalColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPrefilledColumn[column], "Column is a prefilled");

        loaded[column] = true;

        if (hiveColumnIndexes[column] >= value.size()) {
            // this partition may contain fewer fields than what's declared in the schema
            // this happens when additional columns are added to the hive table after a partition has been created
            nulls[column] = true;
        }
        else {
            BytesRefWritable fieldData = value.unCheckedGet(hiveColumnIndexes[column]);

            byte[] bytes;
            try {
                bytes = fieldData.getData();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            int start = fieldData.getStart();
            int length = fieldData.getLength();

            parseDecimalColumn(column, bytes, start, length);
        }
    }

    private void parseDecimalColumn(int column, byte[] bytes, int start, int length)
    {
        if (length == 0) {
            nulls[column] = true;
        }
        else {
            nulls[column] = false;
            decimalWritable.setFromBytes(bytes, start, length);
            DecimalType columnType = (DecimalType) types[column];
            if (columnType.isShort()) {
                longs[column] = getShortDecimalValue(decimalWritable, columnType.getScale());
            }
            else {
                slices[column] = getLongDecimalValue(decimalWritable, columnType.getScale());
            }
        }
    }

    private boolean isValidHiveStringType(HiveType hiveType)
    {
        return hiveType.getCategory() == Category.PRIMITIVE
                && VALID_HIVE_STRING_TYPES.contains(((PrimitiveTypeInfo) hiveType.getTypeInfo()).getPrimitiveCategory());
    }

    @Override
    public Object getObject(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        Type type = types[fieldId];
        if (!isStructuralType(hiveTypes[fieldId])) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(format("Expected field to be structural, actual %s (field %s)", type, fieldId));
        }

        if (!loaded[fieldId]) {
            parseObjectColumn(fieldId);
        }
        return objects[fieldId];
    }

    private void parseObjectColumn(int column)
    {
        // don't include column number in message because it causes boxing which is expensive here
        checkArgument(!isPrefilledColumn[column], "Column is a prefilled");

        loaded[column] = true;

        if (hiveColumnIndexes[column] >= value.size()) {
            // this partition may contain fewer fields than what's declared in the schema
            // this happens when additional columns are added to the hive table after a partition has been created
            nulls[column] = true;
        }
        else {
            BytesRefWritable fieldData = value.unCheckedGet(hiveColumnIndexes[column]);

            byte[] bytes;
            try {
                bytes = fieldData.getData();
            }
            catch (IOException e) {
                throw Throwables.propagate(e);
            }

            int start = fieldData.getStart();
            int length = fieldData.getLength();

            parseObjectColumn(column, bytes, start, length);
        }
    }

    private void parseObjectColumn(int column, byte[] bytes, int start, int length)
    {
        checkState(VALID_HIVE_STRUCTURAL_CATEGORIES.contains(hiveTypes[column].getCategory()), "%s is not a valid STRUCTURAL type", hiveTypes[column]);
        if (length == 0) {
            nulls[column] = true;
        }
        else {
            nulls[column] = false;
            LazyBinaryObject<? extends ObjectInspector> lazyObject = LazyBinaryFactory.createLazyBinaryObject(fieldInspectors[column]);
            ByteArrayRef byteArrayRef = new ByteArrayRef();
            byteArrayRef.setData(bytes);
            lazyObject.init(byteArrayRef, start, length);
            objects[column] = getBlockObject(types[column], lazyObject.getObject(), fieldInspectors[column]);
        }
    }

    @Override
    public boolean isNull(int fieldId)
    {
        checkState(!closed, "Cursor is closed");

        if (!loaded[fieldId]) {
            parseColumn(fieldId);
        }
        return nulls[fieldId];
    }

    private void parseColumn(int column)
    {
        Type type = types[column];
        if (BOOLEAN.equals(type)) {
            parseBooleanColumn(column);
        }
        else if (BIGINT.equals(type)) {
            parseLongColumn(column);
        }
        else if (INTEGER.equals(type)) {
            parseLongColumn(column);
        }
        else if (SMALLINT.equals(type)) {
            parseLongColumn(column);
        }
        else if (TINYINT.equals(type)) {
            parseLongColumn(column);
        }
        else if (DOUBLE.equals(type)) {
            parseDoubleColumn(column);
        }
        else if (REAL.equals(type)) {
            parseLongColumn(column);
        }
        else if (isVarcharType(type) || VARBINARY.equals(type)) {
            parseStringColumn(column);
        }
        else if (isCharType(type)) {
            parseCharColumn(column);
        }
        else if (isStructuralType(hiveTypes[column])) {
            parseObjectColumn(column);
        }
        else if (DATE.equals(type)) {
            parseLongColumn(column);
        }
        else if (TIMESTAMP.equals(type)) {
            parseLongColumn(column);
        }
        else if (type instanceof DecimalType) {
            parseDecimalColumn(column);
        }
        else {
            throw new UnsupportedOperationException("Unsupported column type: " + type);
        }
    }

    private void validateType(int fieldId, Type type)
    {
        if (!types[fieldId].equals(type)) {
            // we don't use Preconditions.checkArgument because it requires boxing fieldId, which affects inner loop performance
            throw new IllegalArgumentException(format("Expected field to be %s, actual %s (field %s)", type, types[fieldId], fieldId));
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
}
