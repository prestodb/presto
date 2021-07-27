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
package com.facebook.presto.maxcompute;

import com.aliyun.odps.Column;
import com.aliyun.odps.NoSuchObjectException;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.OdpsType;
import com.aliyun.odps.PartitionSpec;
import com.aliyun.odps.Table;
import com.aliyun.odps.TableSchema;
import com.aliyun.odps.data.Record;
import com.aliyun.odps.data.RecordReader;
import com.aliyun.odps.data.SimpleStruct;
import com.aliyun.odps.tunnel.TableTunnel;
import com.aliyun.odps.tunnel.TableTunnel.DownloadSession;
import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Decimals;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.SmallintType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.TinyintType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.CharMatcher;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.joda.time.chrono.ISOChronology;

import java.math.BigDecimal;
import java.nio.charset.Charset;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.Decimals.isLongDecimal;
import static com.facebook.presto.common.type.Decimals.isShortDecimal;
import static com.facebook.presto.maxcompute.MaxComputeErrorCode.MAXCOMPUTE_DECIMAL_FORMAT_ERROR;
import static com.facebook.presto.maxcompute.util.MaxComputeReadUtils.serializeObject;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static io.airlift.slice.Slices.wrappedBuffer;
import static java.lang.Float.floatToRawIntBits;
import static java.lang.String.format;
import static java.math.BigDecimal.ROUND_UNNECESSARY;
import static java.util.Objects.requireNonNull;
import static org.joda.time.DateTimeZone.UTC;

public class MaxComputeRecordCursor
        implements RecordCursor
{
    private static final Logger log = Logger.get(MaxComputeRecordCursor.class);

    private final List<MaxComputeColumnHandle> columnHandles;
    private final Map<MaxComputeColumnHandle, Integer> readerColumnIndex;

    private final MaxComputeClient maxComputeClient;
    private final RecordReader recordReader;
    private final BitSet bits;
    private final long startTimeNanos;
    private final String[] prefillValues;
    // ODPS table meta from ODPS perspective.
    private Table table;
    private TableSchema tableSchema;
    private List<Column> columns;
    private List<Column> partColumns;
    private int columnCount;
    // Partition specification of this split.
    private PartitionSpec partSpec;
    private Record record;
    private boolean closed;
    private long completedBytes;
    private boolean isAllPartitionColumns;
    private long recordCount;

    public MaxComputeRecordCursor(ConnectorSession session,
                                  MaxComputeSplit split,
                                  MaxComputeClient maxComputeClient,
                                  List<MaxComputeColumnHandle> columnHandles)
    {
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        this.isAllPartitionColumns = this.columnHandles.stream().allMatch(MaxComputeColumnHandle::isPartitionColumn);

        try {
            this.maxComputeClient = maxComputeClient;
            this.prefillValues = new String[columnHandles.size()];
            bits = new BitSet(columnHandles.size());

            startTimeNanos = System.nanoTime();

            String tableName = split.getTableName();

            // Prepare the ODPS record reader.
            String projectName = split.getProjectName();
            TableTunnel tableTunnel = maxComputeClient.getTableTunnel(session, projectName);
            DownloadSession downloadSession;
            if (split.getPartitionSpecStr() == null) {
                downloadSession = maxComputeClient.getDownloadSession(tableTunnel, projectName, tableName);
            }
            else {
                this.partSpec = new PartitionSpec(split.getPartitionSpecStr());
                downloadSession = maxComputeClient.getDownloadSession(tableTunnel, projectName, tableName, partSpec);
            }
            // Prepare the ODPS table meta.
            Odps odps = maxComputeClient.createOdps();

            try {
                this.table = odps.tables().get(tableName);
                this.table.reload();
                this.tableSchema = this.table.getSchema();
                this.columns = this.table.getSchema().getColumns();
                this.columnCount = this.columns.size();
            }
            catch (NoSuchObjectException e) {
                throw new RuntimeException("Target table was not found: " + tableName, e);
            }
            catch (OdpsException e) {
                throw new RuntimeException("Finding table error: " + tableName + ", due to: " + e.getMessage(), e);
            }

            readerColumnIndex = new HashMap<>();
            if (!isAllPartitionColumns) {
                List<Column> columns = columnHandles.stream()
                        .filter(h -> !h.isPartitionColumn())
                        .map(h -> table.getSchema().getColumn(h.getColumnName()))
                        .collect(Collectors.toList());
                int index = 0;
                for (MaxComputeColumnHandle maxComputeColumnHandle : this.columnHandles) {
                    if (!maxComputeColumnHandle.isPartitionColumn()) {
                        readerColumnIndex.put(maxComputeColumnHandle, index++);
                    }
                }
                this.recordReader = downloadSession.openRecordReader(split.getStart(), split.getCount(), true, columns);
            }
            else {
                this.recordReader = null;
            }
            this.recordCount = split.getCount();

            if (this.table.isExternalTable()) {
                throw new RuntimeException("External table is not supported: " + tableName);
            }

            if (table.isPartitioned()) {
                this.partColumns = tableSchema.getPartitionColumns();
                for (int columnIndex = 0; columnIndex < columnHandles.size(); columnIndex++) {
                    MaxComputeColumnHandle columnHandle = columnHandles.get(columnIndex);
                    if (columnHandle.isPartitionColumn()) {
                        prefillValues[columnIndex] = partSpec.get(columnHandle.getColumnName());
                    }
                }
            }
        }
        catch (Exception e) {
            throw handleSqlException(e);
        }
    }

    public static String simpleStructToString(SimpleStruct struct)
    {
        StringBuilder sb = new StringBuilder();
        sb = simpleStructToStringInner(sb, struct);
        return sb.toString();
    }

    public static StringBuilder simpleStructToStringInner(StringBuilder sb, SimpleStruct struct)
    {
        int fieldCount = struct.getFieldCount();
        sb.append("{");
        for (int i = 0; i < fieldCount; i++) {
            sb.append(struct.getFieldName(i));
            sb.append(":");
            Object fieldValue = struct.getFieldValue(i);
            if (fieldValue instanceof SimpleStruct) {
                sb = simpleStructToStringInner(sb, (SimpleStruct) fieldValue);
            }
            else {
                sb.append(fieldValue.toString());
            }
            sb.append(";");
        }
        if (sb.charAt(sb.length() - 1) == ';') {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("}");
        return sb;
    }

    @Override
    public long getReadTimeNanos()
    {
        return System.nanoTime() - startTimeNanos;
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public Type getType(int field)
    {
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (closed) {
            return false;
        }

        try {
            maxComputeClient.incrementDownloadingThreads();
            if (isAllPartitionColumns) {
                if (recordCount-- <= 0) {
                    close();
                    return false;
                }
                else {
                    bits.clear();
                    return true;
                }
            }
            record = this.recordReader.read();
            if (record == null) {
                close();
                return false;
            }
            else {
                bits.clear();
                return true;
            }
        }
        catch (Exception e) {
            throw handleSqlException(e);
        }
        finally {
            maxComputeClient.decrementDownloadingThreads();
        }
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkState(!closed, "cursor is closed");
        boolean isPartitionColumn = columnHandles.get(field).isPartitionColumn();
        int readerIdx = readerColumnIndex.getOrDefault(columnHandles.get(field), -1);

        try {
            accumulate(field, Byte.BYTES);

            if (isPartitionColumn) {
                return Boolean.parseBoolean(prefillValues[field]);
            }
            else {
                return record.getBoolean(readerIdx);
            }
        }
        catch (Exception e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public long getLong(int field)
    {
        checkState(!closed, "cursor is closed");
        boolean isPartitionColumn = columnHandles.get(field).isPartitionColumn();
        int readerIdx = readerColumnIndex.getOrDefault(columnHandles.get(field), -1);
        try {
            Type type = getType(field);
            if (type.equals(TinyintType.TINYINT) ||
                    type.equals(SmallintType.SMALLINT) ||
                    type.equals(IntegerType.INTEGER) ||
                    type.equals(BigintType.BIGINT)) {
                int bytes = getTypeBytes(type);
                accumulate(field, bytes);

                if (isPartitionColumn) {
                    return Long.parseLong(prefillValues[field]);
                }
                else {
                    Number n = (Number) record.get(readerIdx);
                    return n.longValue();
                }
            }

            if (type.equals(RealType.REAL)) {
                accumulate(field, Float.BYTES);
                if (isPartitionColumn) {
                    // ODPS 的分区类型必须是 string、bigint、int、smallint 以及 tinyint
                    throw new RuntimeException("Invalid ODPS partition column data type: " + type);
                }
                else {
                    Float f = (Float) record.get(readerIdx);
                    return floatToRawIntBits(f);
                }
            }

            if (type.equals(DateType.DATE)) {
                if (isPartitionColumn) {
                    throw new RuntimeException("Invalid ODPS partition column data type: " + type);
                }
                else {
                    // JDBC returns a date using a timestamp at midnight in the JVM timezone
                    long localMillis = record.getDatetime(readerIdx).getTime();
                    // Convert it to a midnight in UTC
                    long utcMillis = ISOChronology.getInstance().getZone().getMillisKeepLocal(UTC, localMillis);
                    // Check out https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html for more details
                    accumulate(field, 3);
                    // convert to days
                    return TimeUnit.MILLISECONDS.toDays(utcMillis);
                }
            }
            // if (type.equals(TimeType.TIME)) {
            //     Time time = resultSet.getTime(field + 1);
            //     // Check out https://dev.mysql.com/doc/refman/8.0/en/storage-requirements.html for more details
            //     accumulate(field, 3);
            //     return UTC_CHRONOLOGY.millisOfDay().get(time.getTime());
            // }
            if (type.equals(TimestampType.TIMESTAMP)) {
                accumulate(field, 4);

                if (isPartitionColumn) {
                    throw new RuntimeException("Invalid ODPS partition column data type: " + type);
                }
                else {
                    Date date = record.getDatetime(readerIdx);
                    return date.getTime();
                }
            }

            if (isShortDecimal(type)) {
                DecimalType shortDecimalType = (DecimalType) type;
                accumulate(field, shortDecimalType.getFixedSize());

                if (isPartitionColumn) {
                    throw new RuntimeException("Invalid ODPS partition column data type: " + type);
                }
                else {
                    BigDecimal decimal = record.getDecimal(readerIdx);
                    String columnName = columnHandles.get(field).getColumnName();
                    BigDecimal formatDecimal = formatDecimal(decimal, shortDecimalType, columnName);
                    return formatDecimal.unscaledValue().longValue();
                }
            }

            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for long: " + type.getTypeSignature());
        }
        catch (Exception e) {
            throw handleSqlException(e);
        }
    }

    private int getTypeBytes(Type type)
    {
        if (type.equals(TinyintType.TINYINT)) {
            return Byte.BYTES;
        }
        else if (type.equals(SmallintType.SMALLINT)) {
            return Short.BYTES;
        }
        else if (type.equals(IntegerType.INTEGER)) {
            return Integer.BYTES;
        }
        else if (type.equals(BigintType.BIGINT)) {
            return Long.BYTES;
        }

        return Long.BYTES;
    }

    private boolean isPartitionColumn(int colIdx)
    {
        return colIdx > this.columnCount - 1;
    }

    @Override
    public double getDouble(int field)
    {
        checkState(!closed, "cursor is closed");
        boolean isPartitionColumn = columnHandles.get(field).isPartitionColumn();
        int colIdx = columnHandles.get(field).getOriginalColumnIndex();
        int readerIdx = readerColumnIndex.getOrDefault(columnHandles.get(field), -1);
        try {
            accumulate(field, Double.BYTES);

            if (isPartitionColumn) {
                int partColIdx = colIdx - this.columnCount;
                if (partColumns.get(partColIdx).getType() == OdpsType.DECIMAL) {
                    return new BigDecimal(prefillValues[field]).doubleValue();
                }
                else {
                    return Double.parseDouble(prefillValues[field]);
                }
            }
            else {
                if (tableSchema.getColumn(colIdx).getType() == OdpsType.DECIMAL) {
                    return record.getDecimal(readerIdx).doubleValue();
                }
                else {
                    return record.getDouble(readerIdx);
                }
            }
        }
        catch (Exception e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Slice getSlice(int field)
    {
        checkState(!closed, "cursor is closed");
        boolean isPartitionColumn = columnHandles.get(field).isPartitionColumn();
        int readerIdx = readerColumnIndex.getOrDefault(columnHandles.get(field), -1);
        try {
            Type type = getType(field);
            if (type instanceof VarcharType) {
                Object object = null;
                if (isPartitionColumn) {
                    object = prefillValues[field];
                }
                else {
                    object = record.get(readerIdx);
                }

                Slice slice = null;
                if (object instanceof SimpleStruct) {
                    slice = utf8Slice(simpleStructToString((SimpleStruct) object));
                }
                else {
                    slice = utf8Slice(object instanceof byte[] ? new String((byte[]) object, Charset.forName("UTF-8")) : object.toString());
                }

                accumulate(field, slice.length());
                return slice;
            }

            if (type instanceof CharType) {
                Object object = null;
                if (isPartitionColumn) {
                    object = prefillValues[field];
                }
                else {
                    object = record.get(readerIdx);
                }

                Slice slice = utf8Slice(CharMatcher.is(' ').trimTrailingFrom(object instanceof byte[] ? new String((byte[]) object, Charset.forName("UTF-8")) : object.toString()));
                accumulate(field, slice.length());
                return slice;
            }

            if (type.equals(VarbinaryType.VARBINARY)) {
                if (isPartitionColumn) {
                    throw new RuntimeException("Invalid ODPS partition column data type: " + type);
                }

                Slice slice = wrappedBuffer(record.getBytes(readerIdx));
                accumulate(field, slice.length());
                return slice;
            }

            if (isLongDecimal(type)) {
                DecimalType longDecimalType = (DecimalType) type;
                accumulate(field, longDecimalType.getFixedSize());

                if (isPartitionColumn) {
                    throw new RuntimeException("Invalid ODPS partition column data type: " + type);
                }
                else {
                    BigDecimal decimal = record.getDecimal(readerIdx);
                    String columnName = columnHandles.get(field).getColumnName();
                    BigDecimal formatDecimal = formatDecimal(decimal, longDecimalType, columnName);
                    return Decimals.encodeUnscaledValue(formatDecimal.unscaledValue());
                }
            }

            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type for slice: " + type.getTypeSignature());
        }
        catch (Exception e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public Object getObject(int field)
    {
        checkState(!closed, "cursor is closed");
        int readerIdx = readerColumnIndex.getOrDefault(columnHandles.get(field), -1);

        Type type = getType(field);
        if (type instanceof ArrayType || type instanceof MapType || type instanceof RowType) {
            return serializeObject(type, null, record.get(readerIdx));
        }

        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkState(!closed, "cursor is closed");
        checkArgument(field < columnHandles.size(), "Invalid field index");
        int colIdx = columnHandles.get(field).getOriginalColumnIndex();
        int readerIdx = readerColumnIndex.getOrDefault(columnHandles.get(field), -1);
        try {
            if (isPartitionColumn(colIdx)) {
                return prefillValues[field] == null;
            }
            else {
                return record.isNull(readerIdx);
            }
        }
        catch (Exception e) {
            throw handleSqlException(e);
        }
    }

    @Override
    public void close()
    {
        if (closed) {
            return;
        }
        closed = true;

        try {
            if (recordReader != null) {
                this.recordReader.close();
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private RuntimeException handleSqlException(Exception e)
    {
        try {
            close();
        }
        catch (Exception closeException) {
            // Self-suppression not permitted
            if (e != closeException) {
                e.addSuppressed(closeException);
            }
        }
        return Throwables.propagate(e);
    }

    /**
     * fieldIdx is 0-based.
     */
    private void accumulate(int fieldIdx, int bytes)
    {
        if (!bits.get(fieldIdx)) {
            completedBytes += bytes;
            bits.set(fieldIdx);
        }
    }

    private static BigDecimal formatDecimal(BigDecimal decimal, DecimalType type, String name)
    {
        try {
            decimal = decimal.setScale(type.getScale(), ROUND_UNNECESSARY);
            if (decimal.precision() > type.getPrecision()) {
                throw new PrestoException(MAXCOMPUTE_DECIMAL_FORMAT_ERROR,
                        format("Invalid decimal value '%s' for %s column name: %s", decimal, type.toString(), name));
            }
            return decimal;
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(MAXCOMPUTE_DECIMAL_FORMAT_ERROR,
                    format("Invalid decimal value '%s' for %s column name: %s", decimal, type.toString(), name));
        }
    }
}
