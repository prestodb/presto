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
package com.facebook.presto.orc.writer;

import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.metadata.CompressionKind;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.statistics.BinaryStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.DateStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.IntegerStatisticsBuilder;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import java.util.List;

import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ColumnWriters
{
    private ColumnWriters() {}

    public static ColumnWriter createColumnWriter(
            int columnIndex,
            List<OrcType> orcTypes,
            Type type,
            CompressionKind compression,
            int bufferSize,
            OrcEncoding orcEncoding,
            DateTimeZone hiveStorageTimeZone,
            DataSize stringStatisticsLimit)
    {
        requireNonNull(type, "type is null");
        OrcType orcType = orcTypes.get(columnIndex);
        switch (orcType.getOrcTypeKind()) {
            case BOOLEAN:
                return new BooleanColumnWriter(columnIndex, type, compression, bufferSize);

            case FLOAT:
                return new FloatColumnWriter(columnIndex, type, compression, bufferSize);

            case DOUBLE:
                return new DoubleColumnWriter(columnIndex, type, compression, bufferSize);

            case BYTE:
                return new ByteColumnWriter(columnIndex, type, compression, bufferSize);

            case DATE:
                checkArgument(orcEncoding != DWRF, "DWRF does not support %s type", type);
                return new LongColumnWriter(columnIndex, type, compression, bufferSize, orcEncoding, DateStatisticsBuilder::new);

            case SHORT:
            case INT:
            case LONG:
                return new LongColumnWriter(columnIndex, type, compression, bufferSize, orcEncoding, IntegerStatisticsBuilder::new);

            case DECIMAL:
                checkArgument(orcEncoding != DWRF, "DWRF does not support %s type", type);
                return new DecimalColumnWriter(columnIndex, type, compression, bufferSize, orcEncoding);

            case TIMESTAMP:
                return new TimestampColumnWriter(columnIndex, type, compression, bufferSize, orcEncoding, hiveStorageTimeZone);

            case BINARY:
                return new SliceDirectColumnWriter(columnIndex, type, compression, bufferSize, orcEncoding, BinaryStatisticsBuilder::new);

            case CHAR:
                checkArgument(orcEncoding != DWRF, "DWRF does not support %s type", type);
                // fall through
            case VARCHAR:
            case STRING:
                return new SliceDictionaryColumnWriter(columnIndex, type, compression, bufferSize, orcEncoding, stringStatisticsLimit);

            case LIST: {
                int fieldColumnIndex = orcType.getFieldTypeIndex(0);
                Type fieldType = type.getTypeParameters().get(0);
                ColumnWriter elementWriter = createColumnWriter(fieldColumnIndex, orcTypes, fieldType, compression, bufferSize, orcEncoding, hiveStorageTimeZone, stringStatisticsLimit);
                return new ListColumnWriter(columnIndex, compression, bufferSize, orcEncoding, elementWriter);
            }

            case MAP: {
                ColumnWriter keyWriter = createColumnWriter(
                        orcType.getFieldTypeIndex(0),
                        orcTypes,
                        type.getTypeParameters().get(0),
                        compression,
                        bufferSize,
                        orcEncoding,
                        hiveStorageTimeZone,
                        stringStatisticsLimit);
                ColumnWriter valueWriter = createColumnWriter(
                        orcType.getFieldTypeIndex(1),
                        orcTypes,
                        type.getTypeParameters().get(1),
                        compression,
                        bufferSize,
                        orcEncoding,
                        hiveStorageTimeZone,
                        stringStatisticsLimit);
                return new MapColumnWriter(columnIndex, compression, bufferSize, orcEncoding, keyWriter, valueWriter);
            }

            case STRUCT: {
                ImmutableList.Builder<ColumnWriter> fieldWriters = ImmutableList.builder();
                for (int fieldId = 0; fieldId < orcType.getFieldCount(); fieldId++) {
                    int fieldColumnIndex = orcType.getFieldTypeIndex(fieldId);
                    Type fieldType = type.getTypeParameters().get(fieldId);
                    fieldWriters.add(createColumnWriter(fieldColumnIndex, orcTypes, fieldType, compression, bufferSize, orcEncoding, hiveStorageTimeZone, stringStatisticsLimit));
                }
                return new StructColumnWriter(columnIndex, compression, bufferSize, fieldWriters.build());
            }
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }
}
