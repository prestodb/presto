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
package com.facebook.presto.hive.parquet.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import parquet.format.ColumnChunk;
import parquet.format.ColumnMetaData;
import parquet.format.ConvertedType;
import parquet.format.Encoding;
import parquet.format.FileMetaData;
import parquet.format.KeyValue;
import parquet.format.RowGroup;
import parquet.format.SchemaElement;
import parquet.format.Statistics;
import parquet.format.Type;
import parquet.hadoop.metadata.BlockMetaData;
import parquet.hadoop.metadata.ColumnChunkMetaData;
import parquet.hadoop.metadata.ColumnPath;
import parquet.hadoop.metadata.CompressionCodecName;
import parquet.hadoop.metadata.ParquetMetadata;
import parquet.schema.MessageType;
import parquet.schema.OriginalType;
import parquet.schema.PrimitiveType.PrimitiveTypeName;
import parquet.schema.Type.Repetition;
import parquet.schema.Types;

import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.hive.parquet.ParquetValidationUtils.validateParquet;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static parquet.format.Util.readFileMetaData;

public final class ParquetMetadataReader
{
    private static final int PARQUET_METADATA_LENGTH = 4;
    private static final byte[] MAGIC = "PAR1".getBytes(US_ASCII);

    private ParquetMetadataReader() {}

    public static ParquetMetadata readFooter(Configuration configuration, Path file)
            throws IOException
    {
        FileSystem fileSystem = file.getFileSystem(configuration);
        FileStatus fileStatus = fileSystem.getFileStatus(file);
        try (FSDataInputStream inputStream = fileSystem.open(file)) {
            // Parquet File Layout:
            //
            // MAGIC
            // variable: Data
            // variable: Metadata
            // 4 bytes: MetadataLength
            // MAGIC

            long length = fileStatus.getLen();
            validateParquet(length >= MAGIC.length + PARQUET_METADATA_LENGTH + MAGIC.length, "%s is not a valid Parquet File", file);
            long metadataLengthIndex = length - PARQUET_METADATA_LENGTH - MAGIC.length;

            inputStream.seek(metadataLengthIndex);
            int metadataLength = readIntLittleEndian(inputStream);

            byte[] magic = new byte[MAGIC.length];
            inputStream.readFully(magic);
            validateParquet(Arrays.equals(MAGIC, magic), "Not valid Parquet file: %s expected magic number: %s got: %s", file, Arrays.toString(MAGIC), Arrays.toString(magic));

            long metadataIndex = metadataLengthIndex - metadataLength;
            validateParquet(
                    metadataIndex >= MAGIC.length && metadataIndex < metadataLengthIndex,
                    "Corrupted Parquet file: %s metadata index: %s out of range",
                    file,
                    metadataIndex);
            inputStream.seek(metadataIndex);
            FileMetaData fileMetaData = readFileMetaData(inputStream);
            List<SchemaElement> schema = fileMetaData.getSchema();
            validateParquet(!schema.isEmpty(), "Empty Parquet schema in file: %s", file);

            MessageType messageType = readParquetSchema(schema);
            List<BlockMetaData> blocks = new ArrayList<>();
            List<RowGroup> rowGroups = fileMetaData.getRow_groups();
            if (rowGroups != null) {
                for (RowGroup rowGroup : rowGroups) {
                    BlockMetaData blockMetaData = new BlockMetaData();
                    blockMetaData.setRowCount(rowGroup.getNum_rows());
                    blockMetaData.setTotalByteSize(rowGroup.getTotal_byte_size());
                    List<ColumnChunk> columns = rowGroup.getColumns();
                    validateParquet(!columns.isEmpty(), "No columns in row group: %s", rowGroup);
                    String filePath = columns.get(0).getFile_path();
                    for (ColumnChunk columnChunk : columns) {
                        validateParquet(
                                (filePath == null && columnChunk.getFile_path() == null)
                                        || (filePath != null && filePath.equals(columnChunk.getFile_path())),
                                "all column chunks of the same row group must be in the same file");
                        ColumnMetaData metaData = columnChunk.meta_data;
                        String[] path = metaData.path_in_schema.toArray(new String[metaData.path_in_schema.size()]);
                        ColumnPath columnPath = ColumnPath.get(path);
                        ColumnChunkMetaData column = ColumnChunkMetaData.get(
                                columnPath,
                                messageType.getType(columnPath.toArray()).asPrimitiveType().getPrimitiveTypeName(),
                                CompressionCodecName.fromParquet(metaData.codec),
                                readEncodings(metaData.encodings),
                                readStats(metaData.statistics, messageType.getType(columnPath.toArray()).asPrimitiveType().getPrimitiveTypeName()),
                                metaData.data_page_offset,
                                metaData.dictionary_page_offset,
                                metaData.num_values,
                                metaData.total_compressed_size,
                                metaData.total_uncompressed_size);
                        blockMetaData.addColumn(column);
                    }
                    blockMetaData.setPath(filePath);
                    blocks.add(blockMetaData);
                }
            }

            Map<String, String> keyValueMetaData = new HashMap<>();
            List<KeyValue> keyValueList = fileMetaData.getKey_value_metadata();
            if (keyValueList != null) {
                for (KeyValue keyValue : keyValueList) {
                    keyValueMetaData.put(keyValue.key, keyValue.value);
                }
            }
            return new ParquetMetadata(new parquet.hadoop.metadata.FileMetaData(messageType, keyValueMetaData, fileMetaData.getCreated_by()), blocks);
        }
    }

    private static MessageType readParquetSchema(List<SchemaElement> schema)
    {
        Iterator<SchemaElement> schemaIterator = schema.iterator();
        SchemaElement rootSchema = schemaIterator.next();
        Types.MessageTypeBuilder builder = Types.buildMessage();
        readTypeSchema(builder, schemaIterator, rootSchema.getNum_children());
        return builder.named(rootSchema.name);
    }

    private static void readTypeSchema(Types.GroupBuilder<?> builder, Iterator<SchemaElement> schemaIterator, int typeCount)
    {
        for (int i = 0; i < typeCount; i++) {
            SchemaElement element = schemaIterator.next();
            Types.Builder<?, ?> typeBuilder;
            if (element.type == null) {
                typeBuilder = builder.group(Repetition.valueOf(element.repetition_type.name()));
                readTypeSchema((Types.GroupBuilder<?>) typeBuilder, schemaIterator, element.num_children);
            }
            else {
                Types.PrimitiveBuilder<?> primitiveBuilder = builder.primitive(getTypeName(element.type), Repetition.valueOf(element.repetition_type.name()));
                if (element.isSetType_length()) {
                    primitiveBuilder.length(element.type_length);
                }
                if (element.isSetPrecision()) {
                    primitiveBuilder.precision(element.precision);
                }
                if (element.isSetScale()) {
                    primitiveBuilder.scale(element.scale);
                }
                typeBuilder = primitiveBuilder;
            }

            if (element.isSetConverted_type()) {
                typeBuilder.as(getOriginalType(element.converted_type));
            }
            if (element.isSetField_id()) {
                typeBuilder.id(element.field_id);
            }
            typeBuilder.named(element.name);
        }
    }

    public static parquet.column.statistics.Statistics<?> readStats(Statistics statistics, PrimitiveTypeName type)
    {
        parquet.column.statistics.Statistics<?> stats = parquet.column.statistics.Statistics.getStatsBasedOnType(type);
        if (statistics != null) {
            if (statistics.isSetMax() && statistics.isSetMin()) {
                stats.setMinMaxFromBytes(statistics.min.array(), statistics.max.array());
            }
            stats.setNumNulls(statistics.null_count);
        }
        return stats;
    }

    private static Set<parquet.column.Encoding> readEncodings(List<Encoding> encodings)
    {
        Set<parquet.column.Encoding> columnEncodings = new HashSet<>();
        for (Encoding encoding : encodings) {
            columnEncodings.add(parquet.column.Encoding.valueOf(encoding.name()));
        }
        return Collections.unmodifiableSet(columnEncodings);
    }

    private static PrimitiveTypeName getTypeName(Type type)
    {
        switch (type) {
            case BYTE_ARRAY:
                return PrimitiveTypeName.BINARY;
            case INT64:
                return PrimitiveTypeName.INT64;
            case INT32:
                return PrimitiveTypeName.INT32;
            case BOOLEAN:
                return PrimitiveTypeName.BOOLEAN;
            case FLOAT:
                return PrimitiveTypeName.FLOAT;
            case DOUBLE:
                return PrimitiveTypeName.DOUBLE;
            case INT96:
                return PrimitiveTypeName.INT96;
            case FIXED_LEN_BYTE_ARRAY:
                return PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY;
            default:
                throw new IllegalArgumentException("Unknown type " + type);
        }
    }

    private static OriginalType getOriginalType(ConvertedType type)
    {
        switch (type) {
            case UTF8:
                return OriginalType.UTF8;
            case MAP:
                return OriginalType.MAP;
            case MAP_KEY_VALUE:
                return OriginalType.MAP_KEY_VALUE;
            case LIST:
                return OriginalType.LIST;
            case ENUM:
                return OriginalType.ENUM;
            case DECIMAL:
                return OriginalType.DECIMAL;
            case DATE:
                return OriginalType.DATE;
            case TIME_MILLIS:
                return OriginalType.TIME_MILLIS;
            case TIMESTAMP_MILLIS:
                return OriginalType.TIMESTAMP_MILLIS;
            case INTERVAL:
                return OriginalType.INTERVAL;
            case INT_8:
                return OriginalType.INT_8;
            case INT_16:
                return OriginalType.INT_16;
            case INT_32:
                return OriginalType.INT_32;
            case INT_64:
                return OriginalType.INT_64;
            case UINT_8:
                return OriginalType.UINT_8;
            case UINT_16:
                return OriginalType.UINT_16;
            case UINT_32:
                return OriginalType.UINT_32;
            case UINT_64:
                return OriginalType.UINT_64;
            case JSON:
                return OriginalType.JSON;
            case BSON:
                return OriginalType.BSON;
            default:
                throw new IllegalArgumentException("Unknown converted type " + type);
        }
    }

    private static int readIntLittleEndian(InputStream in)
            throws IOException
    {
        int ch1 = in.read();
        int ch2 = in.read();
        int ch3 = in.read();
        int ch4 = in.read();
        if ((ch1 | ch2 | ch3 | ch4) < 0) {
            throw new EOFException();
        }
        return ((ch4 << 24) + (ch3 << 16) + (ch2 << 8) + (ch1));
    }
}
