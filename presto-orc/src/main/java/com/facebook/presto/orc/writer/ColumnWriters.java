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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.DwrfEncryptionInfo;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.metadata.CompressionParameters;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.statistics.BinaryStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.DateStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.IntegerStatisticsBuilder;
import com.google.common.collect.ImmutableList;
import io.airlift.units.DataSize;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Optional;

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
            CompressionParameters compressionParameters,
            OrcEncoding orcEncoding,
            DateTimeZone hiveStorageTimeZone,
            DataSize stringStatisticsLimit,
            DwrfEncryptionInfo dwrfEncryptors,
            MetadataWriter metadataWriter)
    {
        requireNonNull(type, "type is null");
        OrcType orcType = orcTypes.get(columnIndex);
        Optional<DwrfDataEncryptor> dwrfEncryptor = dwrfEncryptors.getEncryptorByNodeId(columnIndex);
        switch (orcType.getOrcTypeKind()) {
            case BOOLEAN:
                return new BooleanColumnWriter(columnIndex, type, compressionParameters, dwrfEncryptor, metadataWriter);

            case FLOAT:
                return new FloatColumnWriter(columnIndex, type, compressionParameters, dwrfEncryptor, metadataWriter);

            case DOUBLE:
                return new DoubleColumnWriter(columnIndex, type, compressionParameters, dwrfEncryptor, metadataWriter);

            case BYTE:
                return new ByteColumnWriter(columnIndex, type, compressionParameters, dwrfEncryptor, metadataWriter);

            case DATE:
                checkArgument(orcEncoding != DWRF, "DWRF does not support %s type", type);
                return new LongColumnWriter(columnIndex, type, compressionParameters, dwrfEncryptor, orcEncoding, DateStatisticsBuilder::new, metadataWriter);

            case SHORT:
            case INT:
            case LONG:
                return new LongColumnWriter(columnIndex, type, compressionParameters, dwrfEncryptor, orcEncoding, IntegerStatisticsBuilder::new, metadataWriter);

            case DECIMAL:
                checkArgument(orcEncoding != DWRF, "DWRF does not support %s type", type);
                return new DecimalColumnWriter(columnIndex, type, compressionParameters, orcEncoding, metadataWriter);

            case TIMESTAMP:
                return new TimestampColumnWriter(columnIndex, type, compressionParameters, dwrfEncryptor, orcEncoding, hiveStorageTimeZone, metadataWriter);

            case BINARY:
                return new SliceDirectColumnWriter(columnIndex, type, compressionParameters, dwrfEncryptor, orcEncoding, BinaryStatisticsBuilder::new, metadataWriter);

            case CHAR:
                checkArgument(orcEncoding != DWRF, "DWRF does not support %s type", type);
                // fall through
            case VARCHAR:
            case STRING:
                return new SliceDictionaryColumnWriter(columnIndex, type, compressionParameters, dwrfEncryptor, orcEncoding, stringStatisticsLimit, metadataWriter);

            case LIST: {
                int fieldColumnIndex = orcType.getFieldTypeIndex(0);
                Type fieldType = type.getTypeParameters().get(0);
                ColumnWriter elementWriter = createColumnWriter(
                        fieldColumnIndex,
                        orcTypes,
                        fieldType,
                        compressionParameters,
                        orcEncoding,
                        hiveStorageTimeZone,
                        stringStatisticsLimit,
                        dwrfEncryptors,
                        metadataWriter);
                return new ListColumnWriter(columnIndex, compressionParameters, dwrfEncryptor, orcEncoding, elementWriter, metadataWriter);
            }

            case MAP: {
                ColumnWriter keyWriter = createColumnWriter(
                        orcType.getFieldTypeIndex(0),
                        orcTypes,
                        type.getTypeParameters().get(0),
                        compressionParameters,
                        orcEncoding,
                        hiveStorageTimeZone,
                        stringStatisticsLimit,
                        dwrfEncryptors,
                        metadataWriter);
                ColumnWriter valueWriter = createColumnWriter(
                        orcType.getFieldTypeIndex(1),
                        orcTypes,
                        type.getTypeParameters().get(1),
                        compressionParameters,
                        orcEncoding,
                        hiveStorageTimeZone,
                        stringStatisticsLimit,
                        dwrfEncryptors,
                        metadataWriter);
                return new MapColumnWriter(columnIndex, compressionParameters, dwrfEncryptor, orcEncoding, keyWriter, valueWriter, metadataWriter);
            }

            case STRUCT: {
                ImmutableList.Builder<ColumnWriter> fieldWriters = ImmutableList.builder();
                for (int fieldId = 0; fieldId < orcType.getFieldCount(); fieldId++) {
                    int fieldColumnIndex = orcType.getFieldTypeIndex(fieldId);
                    Type fieldType = type.getTypeParameters().get(fieldId);
                    fieldWriters.add(createColumnWriter(
                            fieldColumnIndex,
                            orcTypes,
                            fieldType,
                            compressionParameters,
                            orcEncoding,
                            hiveStorageTimeZone,
                            stringStatisticsLimit,
                            dwrfEncryptors,
                            metadataWriter));
                }
                return new StructColumnWriter(columnIndex, compressionParameters, dwrfEncryptor, fieldWriters.build(), metadataWriter);
            }
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }
}
