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
package com.facebook.presto.localfile;

import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.ByteSource;
import com.google.common.io.CountingInputStream;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.DateTimeFormatterBuilder;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class LocalFileRecordCursor
        implements RecordCursor
{
    private static final Splitter LINE_SPLITTER = Splitter.on("\t").trimResults();

    // TODO This should be set in the constructor as it may be different for different sources
    public static final DateTimeFormatter ISO_FORMATTER = new DateTimeFormatterBuilder()
            .append(ISODateTimeFormat.dateHourMinuteSecondFraction())
            .appendTimeZoneOffset("Z", true, 2, 2)
            .toFormatter();

    private final int[] fieldToColumnIndex;
    private final HostAddress address;
    private final List<LocalFileColumnHandle> columns;
    private final Iterator<String> lines;
    private final long totalBytes;

    private List<String> fields;

    public LocalFileRecordCursor(List<LocalFileColumnHandle> columns, ByteSource byteSource, HostAddress address)
    {
        this.columns = requireNonNull(columns, "columns is null");
        this.address = requireNonNull(address, "address is null");
        requireNonNull(byteSource, "byteSource is null");

        fieldToColumnIndex = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            LocalFileColumnHandle columnHandle = columns.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        try (CountingInputStream input = new CountingInputStream(byteSource.openStream())) {
            lines = byteSource.asCharSource(UTF_8).readLines().iterator();
            totalBytes = input.getCount();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
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
        return totalBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public Type getType(int field)
    {
        checkArgument(field < columns.size(), "Invalid field index");
        return columns.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!lines.hasNext()) {
            return false;
        }
        // TODO add support for detecting stack traces and treating a stack trace as a single line
        // This will enable us to query any Presto log files
        fields = LINE_SPLITTER.splitToList(lines.next());
        return true;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");

        int columnIndex = fieldToColumnIndex[field];
        if (columnIndex == -1) {
            return address.toString();
        }
        if (columnIndex >= fields.size()) {
            return null;
        }
        return fields.get(columnIndex);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        if (getType(field).equals(TimestampType.TIMESTAMP)) {
            return ISO_FORMATTER.parseDateTime(getFieldValue(field)).getMillis();
        }
        else {
            checkFieldType(field, BigintType.BIGINT);
            return Long.parseLong(getFieldValue(field));
        }
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, VARCHAR);
        return Slices.utf8Slice(getFieldValue(field));
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columns.size(), "Invalid field index");
        String fieldValue = getFieldValue(field);
        return fieldValue.equals("null") || Strings.isNullOrEmpty(fieldValue);
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}
