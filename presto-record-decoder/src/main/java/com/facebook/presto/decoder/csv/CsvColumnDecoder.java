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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.common.type.Varchars.truncateToLength;
import static com.facebook.presto.decoder.DecoderErrorCode.DECODER_CONVERSION_NOT_SUPPORTED;
import static com.facebook.presto.decoder.FieldValueProviders.nullValueProvider;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_USER_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class CsvColumnDecoder
{
    private final String columnName;
    private final Type columnType;
    private final int columnIndex;

    public CsvColumnDecoder(DecoderColumnHandle columnHandle)
    {
        try {
            requireNonNull(columnHandle, "columnHandle is null");
            checkArgument(!columnHandle.isInternal(), "unexpected internal column '%s'", columnHandle.getName());
            columnName = columnHandle.getName();
            checkArgument(columnHandle.getFormatHint() == null, "unexpected format hint '%s' defined for column '%s'", columnHandle.getFormatHint(), columnName);
            checkArgument(columnHandle.getDataFormat() == null, "unexpected data format '%s' defined for column '%s'", columnHandle.getDataFormat(), columnName);
            columnType = columnHandle.getType();

            checkArgument(columnHandle.getMapping() != null, "mapping not defined for column '%s'", columnName);
            try {
                columnIndex = Integer.parseInt(columnHandle.getMapping());
            }
            catch (NumberFormatException e) {
                throw new IllegalArgumentException(format("invalid mapping '%s' for column '%s'", columnHandle.getMapping(), columnName));
            }
            checkArgument(columnIndex >= 0, "invalid mapping '%s' for column '%s'", columnHandle.getMapping(), columnName);

            checkArgument(isSupportedType(columnType), "Unsupported column type '%s' for column '%s'", columnType.getDisplayName(), columnName);
        }
        catch (IllegalArgumentException e) {
            throw new PrestoException(GENERIC_USER_ERROR, e);
        }
    }

    private static boolean isSupportedType(Type type)
    {
        if (isVarcharType(type)) {
            return true;
        }
        if (ImmutableList.of(BIGINT, INTEGER, SMALLINT, TINYINT, BOOLEAN, DOUBLE).contains(type)) {
            return true;
        }
        return false;
    }

    public FieldValueProvider decodeField(String[] tokens)
    {
        if (columnIndex >= tokens.length) {
            return nullValueProvider();
        }
        else {
            return new FieldValueProvider()
            {
                @Override
                public boolean isNull()
                {
                    return tokens[columnIndex].isEmpty();
                }

                @SuppressWarnings("SimplifiableConditionalExpression")
                @Override
                public boolean getBoolean()
                {
                    try {
                        return Boolean.parseBoolean(tokens[columnIndex].trim());
                    }
                    catch (NumberFormatException e) {
                        throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("could not parse value '%s' as '%s' for column '%s'", tokens[columnIndex].trim(), columnType, columnName));
                    }
                }

                @Override
                public long getLong()
                {
                    try {
                        return Long.parseLong(tokens[columnIndex].trim());
                    }
                    catch (NumberFormatException e) {
                        throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("could not parse value '%s' as '%s' for column '%s'", tokens[columnIndex].trim(), columnType, columnName));
                    }
                }

                @Override
                public double getDouble()
                {
                    try {
                        return Double.parseDouble(tokens[columnIndex].trim());
                    }
                    catch (NumberFormatException e) {
                        throw new PrestoException(DECODER_CONVERSION_NOT_SUPPORTED, format("could not parse value '%s' as '%s' for column '%s'", tokens[columnIndex].trim(), columnType, columnName));
                    }
                }

                @Override
                public Slice getSlice()
                {
                    return truncateToLength(utf8Slice(tokens[columnIndex]), columnType);
                }
            };
        }
    }
}
