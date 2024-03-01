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
package com.facebook.presto.lark.sheets;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.lark.sheets.api.LarkSheetsApi;
import com.facebook.presto.lark.sheets.api.SheetValues;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class LarkSheetsRecordCursor
        implements RecordCursor
{
    private final LarkSheetsApi api;
    private final LarkSheetsSplit split;
    private final List<LarkSheetsColumnHandle> readColumns;
    private final int[] indexMapping;

    private List<Object> row;
    private Iterator<List<Object>> rowIterator;
    private long readTimeNanos;
    private long completedBytes;

    public LarkSheetsRecordCursor(
            LarkSheetsApi api,
            LarkSheetsSplit split,
            List<LarkSheetsColumnHandle> readColumns)
    {
        this.api = requireNonNull(api, "api is null");
        this.split = requireNonNull(split, "split is null");
        this.readColumns = requireNonNull(readColumns, "columns is null");
        indexMapping = createIndexMapping(readColumns);
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public Type getType(int field)
    {
        checkFieldIndex(field);
        return readColumns.get(field).getType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (rowIterator == null) {
            rowIterator = loadSheetData();
        }
        if (rowIterator.hasNext()) {
            row = rowIterator.next();
            return true;
        }
        return false;
    }

    @Override
    public boolean getBoolean(int field)
    {
        checkFieldType(field, BOOLEAN);
        Object value = getFieldValue(field);
        return "true".equalsIgnoreCase(value.toString());
    }

    @Override
    public long getLong(int field)
    {
        checkFieldType(field, BIGINT);
        Object value = getFieldValue(field);
        if (value instanceof Number) {
            return ((Number) value).longValue();
        }
        return Long.parseLong(value.toString());
    }

    @Override
    public double getDouble(int field)
    {
        checkFieldType(field, DOUBLE);
        Object value = getFieldValue(field);
        if (value instanceof Number) {
            return ((Number) value).doubleValue();
        }
        return Double.parseDouble(value.toString());
    }

    @Override
    public Slice getSlice(int field)
    {
        checkFieldType(field, VARCHAR);
        return Slices.utf8Slice(getFieldValue(field).toString());
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        return getFieldValue(field) == null;
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return RecordCursor.super.getSystemMemoryUsage();
    }

    @Override
    public void close()
    {
    }

    private void checkFieldIndex(int field)
    {
        checkArgument(field < readColumns.size(), "Invalid field index");
    }

    private Object getFieldValue(int field)
    {
        checkState(row != null, "Cursor has not been advanced yet");
        return row.get(indexMapping[field]);
    }

    private void checkFieldType(int field, Type expected)
    {
        Type actual = getType(field);
        checkArgument(actual.equals(expected), "Expected field %s to be type %s but is %s", field, expected, actual);
    }

    private Iterator<List<Object>> loadSheetData()
    {
        LarkSheetsTableHandle table = split.getTable();

        // load remote data
        long readStartTime = System.nanoTime();
        SheetValues sheetValues;
        try {
            sheetValues = api.getValues(table.getSpreadsheetToken(), table.getSheetId());
        }
        finally {
            readTimeNanos = System.nanoTime() - readStartTime;
        }

        List<List<Object>> values = sheetValues.getValues();
        if (values.size() <= 1) {
            // sheets that only have the header row
            completedBytes = 0;
            return ImmutableList.<List<Object>>of().iterator();
        }
        values = values.subList(1, values.size());
        completedBytes = estimateDataSize(values);

        return values.iterator();
    }

    private long estimateDataSize(List<List<Object>> values)
    {
        // estimate data size
        long sum = 0;
        for (List<Object> row : values) {
            for (Object field : row) {
                if (field instanceof Double) {
                    sum += 8;
                }
                else if (field instanceof String) {
                    sum += ((String) field).length();
                }
            }
        }
        return sum;
    }

    private static int[] createIndexMapping(List<LarkSheetsColumnHandle> readColumns)
    {
        int numColumns = readColumns.size();
        int[] indexes = new int[numColumns];
        for (int i = 0; i < numColumns; i++) {
            indexes[i] = readColumns.get(i).getIndex();
        }
        return indexes;
    }
}
