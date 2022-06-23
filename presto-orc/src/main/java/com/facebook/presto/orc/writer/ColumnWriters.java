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
import com.facebook.presto.orc.ColumnWriterOptions;
import com.facebook.presto.orc.DwrfDataEncryptor;
import com.facebook.presto.orc.DwrfEncryptionInfo;
import com.facebook.presto.orc.OrcEncoding;
import com.facebook.presto.orc.metadata.MetadataWriter;
import com.facebook.presto.orc.metadata.OrcType;
import com.facebook.presto.orc.metadata.statistics.BinaryStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.ColumnStatistics;
import com.facebook.presto.orc.metadata.statistics.DateStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.IntegerStatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.StatisticsBuilder;
import com.facebook.presto.orc.metadata.statistics.StringStatisticsBuilder;
import com.google.common.collect.ImmutableList;
import org.joda.time.DateTimeZone;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.IntFunction;
import java.util.function.Supplier;

import static com.facebook.presto.orc.OrcEncoding.DWRF;
import static com.facebook.presto.orc.metadata.ColumnEncoding.DEFAULT_SEQUENCE_ID;
import static com.facebook.presto.orc.metadata.statistics.StatisticsBuilders.createEmptyColumnStatistics;
import static com.facebook.presto.orc.metadata.statistics.StatisticsBuilders.createStatisticsBuilderSupplier;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public final class ColumnWriters
{
    private ColumnWriters() {}

    /**
     * Creates a column writer for a given type.
     *
     * @param nodeIndex - index of the node in the orcTypes
     */
    public static ColumnWriter createColumnWriter(
            int nodeIndex,
            int sequence,
            List<OrcType> orcTypes,
            Type type,
            ColumnWriterOptions columnWriterOptions,
            OrcEncoding orcEncoding,
            DateTimeZone hiveStorageTimeZone,
            DwrfEncryptionInfo dwrfEncryptors,
            MetadataWriter metadataWriter)
    {
        requireNonNull(type, "type is null");
        OrcType orcType = orcTypes.get(nodeIndex);
        Optional<DwrfDataEncryptor> dwrfEncryptor = dwrfEncryptors.getEncryptorByNodeId(nodeIndex);
        switch (orcType.getOrcTypeKind()) {
            case BOOLEAN:
                return new BooleanColumnWriter(nodeIndex, sequence, type, columnWriterOptions, dwrfEncryptor, metadataWriter);

            case FLOAT:
                return new FloatColumnWriter(nodeIndex, sequence, type, columnWriterOptions, dwrfEncryptor, metadataWriter);

            case DOUBLE:
                return new DoubleColumnWriter(nodeIndex, sequence, type, columnWriterOptions, dwrfEncryptor, metadataWriter);

            case BYTE:
                return new ByteColumnWriter(nodeIndex, sequence, type, columnWriterOptions, dwrfEncryptor, metadataWriter);

            case DATE:
                checkArgument(orcEncoding != DWRF, "DWRF does not support %s type", type);
                return new LongColumnWriter(nodeIndex, DEFAULT_SEQUENCE_ID, type, columnWriterOptions, dwrfEncryptor, orcEncoding, DateStatisticsBuilder::new, metadataWriter);

            case SHORT:
                return new LongColumnWriter(nodeIndex, sequence, type, columnWriterOptions, dwrfEncryptor, orcEncoding, IntegerStatisticsBuilder::new, metadataWriter);
            case INT:
            case LONG:
                if (columnWriterOptions.isIntegerDictionaryEncodingEnabled() && orcEncoding == DWRF) {
                    // ORC V1 does not support Integer Dictionary encoding. DWRF supports Integer dictionary encoding.
                    return new LongDictionaryColumnWriter(nodeIndex, sequence, type, columnWriterOptions, dwrfEncryptor, orcEncoding, metadataWriter);
                }
                return new LongColumnWriter(nodeIndex, sequence, type, columnWriterOptions, dwrfEncryptor, orcEncoding, IntegerStatisticsBuilder::new, metadataWriter);

            case DECIMAL:
                checkArgument(orcEncoding != DWRF, "DWRF does not support %s type", type);
                return new DecimalColumnWriter(nodeIndex, type, columnWriterOptions, orcEncoding, metadataWriter);

            case TIMESTAMP:
            case TIMESTAMP_MICROSECONDS:
                return new TimestampColumnWriter(nodeIndex, sequence, type, columnWriterOptions, dwrfEncryptor, orcEncoding, hiveStorageTimeZone, metadataWriter);

            case BINARY:
                return new SliceDirectColumnWriter(nodeIndex, sequence, type, columnWriterOptions, dwrfEncryptor, orcEncoding, BinaryStatisticsBuilder::new, metadataWriter);

            case CHAR:
                checkArgument(orcEncoding != DWRF, "DWRF does not support %s type", type);
                // fall through
            case VARCHAR:
            case STRING:
                if (columnWriterOptions.isStringDictionaryEncodingEnabled()) {
                    return new SliceDictionaryColumnWriter(nodeIndex, sequence, type, columnWriterOptions, dwrfEncryptor, orcEncoding, metadataWriter);
                }
                int stringStatisticsLimit = columnWriterOptions.getStringStatisticsLimit();
                return new SliceDirectColumnWriter(
                        nodeIndex,
                        sequence,
                        type,
                        columnWriterOptions,
                        dwrfEncryptor,
                        orcEncoding,
                        () -> new StringStatisticsBuilder(stringStatisticsLimit),
                        metadataWriter);

            case LIST: {
                Type fieldType = type.getTypeParameters().get(0);
                ColumnWriter elementWriter = createColumnWriter(
                        orcType.getFieldTypeIndex(0),
                        sequence,
                        orcTypes,
                        fieldType,
                        columnWriterOptions,
                        orcEncoding,
                        hiveStorageTimeZone,
                        dwrfEncryptors,
                        metadataWriter);
                return new ListColumnWriter(nodeIndex, sequence, columnWriterOptions, dwrfEncryptor, orcEncoding, elementWriter, metadataWriter);
            }

            case MAP: {
                if (columnWriterOptions.getFlattenedNodes().contains(nodeIndex)) {
                    checkArgument(orcEncoding == DWRF, "%s does not support flat maps", orcEncoding);
                    Type valueType = type.getTypeParameters().get(1);
                    OrcType keyOrcType = orcTypes.get(orcType.getFieldTypeIndex(0));
                    Supplier<StatisticsBuilder> keyStatisticsBuilderSupplier = createStatisticsBuilderSupplier(keyOrcType, columnWriterOptions);

                    // value writers should not create their own expensive dictionaries, instead they should use shared dictionaries
                    ColumnWriterOptions valueWriterColumnWriterOptions = columnWriterOptions.copyWithDisabledDictionaryEncoding();

                    // Flat map writer needs to provide column statistics for the value node(s) even if there are no values.
                    // This lambda will provide empty column statistics right away instead of creating an expensive empty
                    // value writer and getting empty stats from it.
                    int valueNodeIndex = orcType.getFieldTypeIndex(1);
                    Supplier<Map<Integer, ColumnStatistics>> emptyColumnStatisticsSupplier =
                            () -> createEmptyColumnStatistics(orcTypes, valueNodeIndex, columnWriterOptions);

                    IntFunction<ColumnWriter> valueWriterSupplier = (valueSequence) -> createColumnWriter(
                            valueNodeIndex,
                            valueSequence,
                            orcTypes,
                            valueType,
                            valueWriterColumnWriterOptions,
                            orcEncoding,
                            hiveStorageTimeZone,
                            dwrfEncryptors,
                            metadataWriter);

                    return new MapFlatColumnWriter(
                            nodeIndex,
                            orcType.getFieldTypeIndex(0),
                            valueNodeIndex,
                            type.getTypeParameters().get(0),
                            valueType,
                            keyStatisticsBuilderSupplier,
                            columnWriterOptions,
                            dwrfEncryptor,
                            metadataWriter,
                            valueWriterSupplier,
                            emptyColumnStatisticsSupplier);
                }

                ColumnWriter keyWriter = createColumnWriter(
                        orcType.getFieldTypeIndex(0),
                        sequence,
                        orcTypes,
                        type.getTypeParameters().get(0),
                        columnWriterOptions,
                        orcEncoding,
                        hiveStorageTimeZone,
                        dwrfEncryptors,
                        metadataWriter);
                ColumnWriter valueWriter = createColumnWriter(
                        orcType.getFieldTypeIndex(1),
                        sequence,
                        orcTypes,
                        type.getTypeParameters().get(1),
                        columnWriterOptions,
                        orcEncoding,
                        hiveStorageTimeZone,
                        dwrfEncryptors,
                        metadataWriter);
                return new MapColumnWriter(nodeIndex, sequence, columnWriterOptions, dwrfEncryptor, orcEncoding, keyWriter, valueWriter, metadataWriter);
            }

            case STRUCT: {
                ImmutableList.Builder<ColumnWriter> fieldWriters = ImmutableList.builder();
                for (int fieldId = 0; fieldId < orcType.getFieldCount(); fieldId++) {
                    int childNodeIndex = orcType.getFieldTypeIndex(fieldId);
                    Type fieldType = type.getTypeParameters().get(fieldId);
                    fieldWriters.add(createColumnWriter(
                            childNodeIndex,
                            sequence,
                            orcTypes,
                            fieldType,
                            columnWriterOptions,
                            orcEncoding,
                            hiveStorageTimeZone,
                            dwrfEncryptors,
                            metadataWriter));
                }
                return new StructColumnWriter(nodeIndex, sequence, columnWriterOptions, dwrfEncryptor, fieldWriters.build(), metadataWriter);
            }
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }
}
