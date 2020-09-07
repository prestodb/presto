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
package com.facebook.presto.parquet.writer;

import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.parquet.writer.valuewriter.BigintValueWriter;
import com.facebook.presto.parquet.writer.valuewriter.BooleanValueWriter;
import com.facebook.presto.parquet.writer.valuewriter.CharValueWriter;
import com.facebook.presto.parquet.writer.valuewriter.DateValueWriter;
import com.facebook.presto.parquet.writer.valuewriter.DecimalValueWriter;
import com.facebook.presto.parquet.writer.valuewriter.DoubleValueWriter;
import com.facebook.presto.parquet.writer.valuewriter.IntegerValueWriter;
import com.facebook.presto.parquet.writer.valuewriter.PrimitiveValueWriter;
import com.facebook.presto.parquet.writer.valuewriter.RealValueWriter;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.column.values.ValuesWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

class ParquetWriters
{
    private ParquetWriters() {}

    static List<ColumnWriter> getColumnWriters(MessageType messageType, Map<List<String>, Type> prestoTypes, ParquetProperties parquetProperties, CompressionCodecName compressionCodecName)
    {
        WriterBuilder writeBuilder = new WriterBuilder(messageType, prestoTypes, parquetProperties, compressionCodecName);
        ParquetTypeVisitor.visit(messageType, writeBuilder);
        return writeBuilder.build();
    }

    private static class WriterBuilder
            extends ParquetTypeVisitor<ColumnWriter>
    {
        private final MessageType type;
        private final Map<List<String>, Type> prestoTypes;
        private final ParquetProperties parquetProperties;
        private final CompressionCodecName compressionCodecName;
        private final ImmutableList.Builder<ColumnWriter> builder = ImmutableList.builder();

        WriterBuilder(MessageType messageType, Map<List<String>, Type> prestoTypes, ParquetProperties parquetProperties, CompressionCodecName compressionCodecName)
        {
            this.type = requireNonNull(messageType, "messageType is null");
            this.prestoTypes = requireNonNull(prestoTypes, "prestoTypes is null");
            this.parquetProperties = requireNonNull(parquetProperties, "parquetProperties is null");
            this.compressionCodecName = requireNonNull(compressionCodecName, "compressionCodecName is null");
        }

        List<ColumnWriter> build()
        {
            return builder.build();
        }

        @Override
        public ColumnWriter message(MessageType message, List<ColumnWriter> fields)
        {
            builder.addAll(fields);
            return super.message(message, fields);
        }

        @Override
        public ColumnWriter struct(GroupType struct, List<ColumnWriter> fields)
        {
            String[] path = currentPath();
            int fieldDefinitionLevel = type.getMaxDefinitionLevel(path);
            int fieldRepetitionLevel = type.getMaxRepetitionLevel(path);
            return new StructColumnWriter(ImmutableList.copyOf(fields), fieldDefinitionLevel, fieldRepetitionLevel);
        }

        @Override
        public ColumnWriter list(GroupType array, ColumnWriter element)
        {
            String[] path = currentPath();
            int fieldDefinitionLevel = type.getMaxDefinitionLevel(path);
            int fieldRepetitionLevel = type.getMaxRepetitionLevel(path);
            return new ArrayColumnWriter(element, fieldDefinitionLevel, fieldRepetitionLevel);
        }

        @Override
        public ColumnWriter map(GroupType map, ColumnWriter key, ColumnWriter value)
        {
            String[] path = currentPath();
            int fieldDefinitionLevel = type.getMaxDefinitionLevel(path);
            int fieldRepetitionLevel = type.getMaxRepetitionLevel(path);
            return new MapColumnWriter(key, value, fieldDefinitionLevel, fieldRepetitionLevel);
        }

        @Override
        public ColumnWriter primitive(PrimitiveType primitive)
        {
            String[] path = currentPath();
            int fieldDefinitionLevel = type.getMaxDefinitionLevel(path);
            int fieldRepetitionLevel = type.getMaxRepetitionLevel(path);
            ColumnDescriptor columnDescriptor = new ColumnDescriptor(path, primitive, fieldRepetitionLevel, fieldDefinitionLevel);
            Type prestoType = requireNonNull(prestoTypes.get(ImmutableList.copyOf(path)), " presto type is null");
            return new PrimitiveColumnWriter(prestoType,
                    columnDescriptor,
                    getValueWriter(parquetProperties.newValuesWriter(columnDescriptor), prestoType, columnDescriptor.getPrimitiveType()),
                    parquetProperties.newDefinitionLevelEncoder(columnDescriptor),
                    parquetProperties.newRepetitionLevelEncoder(columnDescriptor),
                    compressionCodecName,
                    parquetProperties.getPageSizeThreshold());
        }

        private String[] currentPath()
        {
            String[] path = new String[fieldNames.size()];
            if (!fieldNames.isEmpty()) {
                Iterator<String> iter = fieldNames.descendingIterator();
                for (int i = 0; iter.hasNext(); i += 1) {
                    path[i] = iter.next();
                }
            }
            return path;
        }
    }

    private static PrimitiveValueWriter getValueWriter(ValuesWriter valuesWriter, Type type, PrimitiveType parquetType)
    {
        if (BOOLEAN.equals(type)) {
            return new BooleanValueWriter(valuesWriter, parquetType);
        }
        if (INTEGER.equals(type) || SMALLINT.equals(type) || TINYINT.equals(type)) {
            return new IntegerValueWriter(valuesWriter, type, parquetType);
        }
        if (type instanceof DecimalType) {
            return new DecimalValueWriter(valuesWriter, type, parquetType);
        }
        if (DATE.equals(type)) {
            return new DateValueWriter(valuesWriter, parquetType);
        }
        if (BIGINT.equals(type) || TIMESTAMP.equals(type)) {
            return new BigintValueWriter(valuesWriter, type, parquetType);
        }
        if (DOUBLE.equals(type)) {
            return new DoubleValueWriter(valuesWriter, parquetType);
        }
        if (REAL.equals(type)) {
            return new RealValueWriter(valuesWriter, parquetType);
        }
        if (type instanceof VarcharType || type instanceof CharType || type instanceof VarbinaryType) {
            return new CharValueWriter(valuesWriter, type, parquetType);
        }
        throw new PrestoException(NOT_SUPPORTED, format("Unsupported type for Parquet writer: %s", type));
    }
}
