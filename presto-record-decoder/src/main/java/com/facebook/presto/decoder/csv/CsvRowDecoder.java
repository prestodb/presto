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
package com.facebook.presto.decoder.csv;

import au.com.bytecode.opencsv.CSVParser;
import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

/**
 * Decode row as CSV. This is an extremely primitive CSV decoder using {@link au.com.bytecode.opencsv.CSVParser]}.
 */
public class CsvRowDecoder
        implements RowDecoder
{
    public static final String NAME = "csv";

    private final Set<DecoderColumnHandle> columnHandles;
    private final CSVParser parser = new CSVParser();

    public CsvRowDecoder(Set<DecoderColumnHandle> columnHandles)
    {
        requireNonNull(columnHandles, "columnHandles is null");
        checkArgument(columnHandles.stream().noneMatch(DecoderColumnHandle::isInternal), "unexpected internal column");
        this.columnHandles = ImmutableSet.copyOf(columnHandles);
    }

    @Override
    public Optional<Map<DecoderColumnHandle, FieldValueProvider>> decodeRow(byte[] data, Map<String, String> dataMap)
    {
        String[] fields;
        try {
            // TODO - There is no reason why the row can't have a formatHint and it could be used
            // to set the charset here.
            String line = new String(data, StandardCharsets.UTF_8);
            fields = parser.parseLine(line);
        }
        catch (Exception e) {
            return Optional.empty();
        }

        Map<DecoderColumnHandle, FieldValueProvider> decodedRow = new HashMap<>();
        for (DecoderColumnHandle columnHandle : columnHandles) {
            if (columnHandle.isInternal()) {
                continue;
            }

            String mapping = columnHandle.getMapping();
            checkState(mapping != null, "No mapping for column handle %s!", columnHandle);
            int columnIndex = Integer.parseInt(mapping);

            if (columnIndex >= fields.length) {
                continue;
            }

            decodedRow.put(columnHandle, decodeField(fields[columnIndex], columnHandle));
        }
        return Optional.of(decodedRow);
    }

    private static FieldValueProvider decodeField(String value, DecoderColumnHandle columnHandle)
    {
        requireNonNull(columnHandle, "columnHandle is null");

        return new FieldValueProvider()
        {
            @Override
            public boolean isNull()
            {
                return value.isEmpty();
            }

            @SuppressWarnings("SimplifiableConditionalExpression")
            @Override
            public boolean getBoolean()
            {
                return Boolean.parseBoolean(value.trim());
            }

            @Override
            public long getLong()
            {
                return Long.parseLong(value.trim());
            }

            @Override
            public double getDouble()
            {
                return Double.parseDouble(value.trim());
            }

            @Override
            public Slice getSlice()
            {
                Slice slice = utf8Slice(value);
                if (isVarcharType(columnHandle.getType())) {
                    slice = truncateToLength(slice, columnHandle.getType());
                }
                return slice;
            }
        };
    }
}
