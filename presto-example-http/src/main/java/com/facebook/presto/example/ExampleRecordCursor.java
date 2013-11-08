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
package com.facebook.presto.example;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.Charsets;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.io.CountingInputStream;
import com.google.common.io.InputSupplier;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.io.ByteStreams.asByteSource;

public class ExampleRecordCursor
        implements RecordCursor
{
    private static final Splitter LINE_SPLITTER = Splitter.on(",").trimResults();

    private final List<ExampleColumnHandle> columnHandles;
    private final int[] fieldToColumnIndex;

    private final Iterator<String> lines;
    private final long totalBytes;

    private List<String> fields;

    public ExampleRecordCursor(List<ExampleColumnHandle> columnHandles, InputSupplier<InputStream> inputStreamSupplier)
    {
        this.columnHandles = columnHandles;

        fieldToColumnIndex = new int[columnHandles.size()];
        for (int i = 0; i < columnHandles.size(); i++) {
            ExampleColumnHandle columnHandle = columnHandles.get(i);
            fieldToColumnIndex[i] = columnHandle.getOrdinalPosition();
        }

        try (CountingInputStream input = new CountingInputStream(inputStreamSupplier.getInput())) {
            lines = asByteSource(inputStreamSupplier).asCharSource(StandardCharsets.UTF_8).readLines().iterator();
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
    public ColumnType getType(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!lines.hasNext()) {
            return false;
        }
        String line = lines.next();
        fields = LINE_SPLITTER.splitToList(line);

        return true;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yes");

        int columnIndex = fieldToColumnIndex[field];
        return fields.get(columnIndex);
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, ColumnType.BOOLEAN);
        return Boolean.parseBoolean(getFieldValue(field));
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, ColumnType.LONG);
        return Long.parseLong(getFieldValue(field));
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, ColumnType.DOUBLE);
        return Double.parseDouble(getFieldValue(field));
    }

    @Override
    public byte[] getString(int field)
    {
        checkFieldType(field, ColumnType.STRING);
        return getFieldValue(field).getBytes(Charsets.UTF_8);
    }

    @Override
    public boolean isNull(int field)
    {
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
    }

    private void checkFieldType(int field, ColumnType expected)
    {
        ColumnType actual = getType(field);
        checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    @Override
    public void close()
    {
    }
}
