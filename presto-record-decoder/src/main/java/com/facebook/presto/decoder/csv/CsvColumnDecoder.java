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

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import io.airlift.slice.Slice;

import static com.facebook.presto.decoder.FieldValueProviders.nullValueProvider;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.facebook.presto.spi.type.Varchars.truncateToLength;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public class CsvColumnDecoder
{
    private final DecoderColumnHandle columnHandle;

    public CsvColumnDecoder(DecoderColumnHandle columnHandle)
    {
        this.columnHandle = requireNonNull(columnHandle, "columnHandle is null");
        checkArgument(!columnHandle.isInternal(), "unexpected internal column '%s'", columnHandle.getName());
    }

    public FieldValueProvider decodeField(String[] tokens)
    {
        String mapping = columnHandle.getMapping();
        checkState(mapping != null, "No mapping for column handle %s!", columnHandle);
        int columnIndex = Integer.parseInt(mapping);

        if (columnIndex >= tokens.length) {
            return nullValueProvider();
        }
        else {
            return decodeField(tokens[columnIndex], columnHandle);
        }
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
