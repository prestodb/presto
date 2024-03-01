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
package com.facebook.presto.google.sheets;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Arrays;
import java.util.List;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.createUnboundedVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class SheetsRecordCursor
        implements RecordCursor
{
    private final List<SheetsColumnHandle> columnHandles;
    private final long totalBytes;
    private final List<List<Object>> dataValues;

    private List<String> fields;
    private int currentIndex;

    public SheetsRecordCursor(List<SheetsColumnHandle> columnHandles, List<List<Object>> dataValues)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        requireNonNull(dataValues, "dataValues is null");

        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.dataValues = ImmutableList.copyOf(dataValues);
        long inputLength = 0;
        for (List<Object> objList : dataValues) {
            for (Object obj : objList) {
                inputLength += String.valueOf(obj).length();
            }
        }
        totalBytes = inputLength;
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
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return columnHandles.get(field).getColumnType();
    }

    @Override
    public boolean advanceNextPosition()
    {
        List<Object> currentVals = null;
        // Skip empty rows from sheet
        while (currentVals == null || currentVals.size() == 0) {
            if (currentIndex == dataValues.size()) {
                return false;
            }
            currentVals = dataValues.get(currentIndex++);
        }
        // Populate incomplete columns with null
        String[] allFields = new String[columnHandles.size()];

        for (int i = 0; i < allFields.length; i++) {
            int ordinalPos = columnHandles.get(i).getOrdinalPosition();
            if (currentVals.size() > ordinalPos) {
                allFields[i] = String.valueOf(currentVals.get(ordinalPos));
            }
        }
        fields = Arrays.asList(allFields);
        return true;
    }

    private String getFieldValue(int field)
    {
        checkState(fields != null, "Cursor has not been advanced yet");
        return fields.get(field);
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
        checkFieldType(field, BIGINT);
        return Long.parseLong(getFieldValue(field));
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
        checkFieldType(field, createUnboundedVarcharType());
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
        checkArgument(field < columnHandles.size(), "Invalid field index");
        return Strings.isNullOrEmpty(getFieldValue(field));
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
