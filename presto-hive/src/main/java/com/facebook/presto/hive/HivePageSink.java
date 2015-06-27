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
package com.facebook.presto.hive;

import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;
import io.airlift.slice.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.hive.HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_ERROR;
import static com.facebook.presto.hive.HiveWriteUtils.getField;
import static com.facebook.presto.hive.HiveWriteUtils.getJavaObjectInspectors;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;

public class HivePageSink
        implements ConnectorPageSink
{
    private final String schemaName;
    private final String tableName;

    private final int[] dataColumns;
    private final int sampleWeightDataColumn;
    private final List<String> dataColumnNames;
    private final List<Type> dataColumnTypes;

    private final HiveStorageFormat tableStorageFormat;
    private final Path writePath;
    private final String filePrefix;

    private final TypeManager typeManager;
    private final JobConf conf;

    private final List<Object> dataRow;

    private HiveRecordWriter writer;

    public HivePageSink(
            String schemaName,
            String tableName,
            List<HiveColumnHandle> inputColumns,
            HiveStorageFormat tableStorageFormat,
            Path writePath,
            String filePrefix,
            TypeManager typeManager,
            HdfsEnvironment hdfsEnvironment)
    {
        this.schemaName = requireNonNull(schemaName, "schemaName is null");
        this.tableName = requireNonNull(tableName, "tableName is null");

        requireNonNull(inputColumns, "inputColumns is null");

        this.tableStorageFormat = requireNonNull(tableStorageFormat, "tableStorageFormat is null");
        this.writePath = requireNonNull(writePath, "writePath is null");
        this.filePrefix = requireNonNull(filePrefix, "filePrefix is null");

        this.typeManager = requireNonNull(typeManager, "typeManager is null");

        ImmutableList.Builder<String> dataColumnNames = ImmutableList.builder();
        ImmutableList.Builder<Type> dataColumnTypes = ImmutableList.builder();
        for (HiveColumnHandle column : inputColumns) {
            dataColumnNames.add(column.getName());
            dataColumnTypes.add(typeManager.getType(column.getTypeSignature()));
        }
        this.dataColumnNames = dataColumnNames.build();
        this.dataColumnTypes = dataColumnTypes.build();

        ImmutableList.Builder<Integer> dataColumns = ImmutableList.builder();
        // sample weight column is passed separately, so index must be calculated without this column
        List<HiveColumnHandle> inputColumnsWithoutSample = inputColumns.stream()
                .filter(column -> !column.getName().equals(SAMPLE_WEIGHT_COLUMN_NAME))
                .collect(toList());
        for (int inputIndex = 0; inputIndex < inputColumnsWithoutSample.size(); inputIndex++) {
            dataColumns.add(inputIndex);
        }
        this.dataColumns = Ints.toArray(dataColumns.build());
        this.sampleWeightDataColumn = this.dataColumnNames.indexOf(SAMPLE_WEIGHT_COLUMN_NAME);

        // preallocate temp space for data
        this.dataRow = Arrays.asList(new Object[this.dataColumnNames.size()]);

        this.conf = new JobConf(hdfsEnvironment.getConfiguration(writePath));
    }

    @Override
    public Collection<Slice> commit()
    {
        if (writer != null) {
            writer.commit();
        }
        return ImmutableList.of();
    }

    @Override
    public void rollback()
    {
        if (writer != null) {
            writer.rollback();
        }
    }

    @Override
    public void appendPage(Page page, Block sampleWeightBlock)
    {
        if (page.getPositionCount() == 0) {
            return;
        }

        Block[] dataBlocks = getDataBlocks(page, sampleWeightBlock);

        if (writer == null) {
            writer = createWriter();
        }

        for (int position = 0; position < page.getPositionCount(); position++) {
            buildRow(dataColumnTypes, dataRow, dataBlocks, position);
            writer.addRow(dataRow);
        }
    }

    private HiveRecordWriter createWriter()
    {
        Properties schema = new Properties();
        schema.setProperty(META_TABLE_COLUMNS, Joiner.on(',').join(dataColumnNames));
        schema.setProperty(META_TABLE_COLUMN_TYPES, dataColumnTypes.stream()
                .map(HiveType::toHiveType)
                .map(HiveType::getHiveTypeName)
                .collect(Collectors.joining(":")));

        return new HiveRecordWriter(
                schemaName,
                tableName,
                dataColumnNames,
                tableStorageFormat.getOutputFormat(),
                tableStorageFormat.getSerDe(),
                schema,
                generateRandomFileName(),
                writePath,
                typeManager,
                conf);
    }

    private String generateRandomFileName()
    {
        return filePrefix + "_" + randomUUID();
    }

    private Block[] getDataBlocks(Page page, Block sampleWeightBlock)
    {
        Block[] blocks = new Block[dataColumns.length + (sampleWeightBlock != null ? 1 : 0)];
        for (int i = 0; i < dataColumns.length; i++) {
            int dataColumn = dataColumns[i];
            blocks[i] = page.getBlock(dataColumn);
        }
        if (sampleWeightBlock != null) {
            blocks[sampleWeightDataColumn] = sampleWeightBlock;
        }
        return blocks;
    }

    private static void buildRow(List<Type> columnTypes, List<Object> row, Block[] blocks, int position)
    {
        for (int field = 0; field < blocks.length; field++) {
            row.set(field, getField(columnTypes.get(field), blocks[field], position));
        }
    }

    private static class HiveRecordWriter
    {
        private final String fileName;
        private final Path writePath;
        private final int fieldCount;
        @SuppressWarnings("deprecation")
        private final Serializer serializer;
        private final RecordWriter recordWriter;
        private final SettableStructObjectInspector tableInspector;
        private final List<StructField> structFields;
        private final Object row;

        public HiveRecordWriter(
                String schemaName,
                String tableName,
                List<String> inputColumnNames,
                String outputFormat,
                String serDe,
                Properties schema,
                String fileName,
                Path writePath,
                TypeManager typeManager,
                JobConf conf)
        {
            this.fileName = fileName;
            this.writePath = writePath;

            // existing tables may have columns in a different order
            List<String> fileColumnNames = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(schema.getProperty(META_TABLE_COLUMNS, ""));
            List<Type> fileColumnTypes = Splitter.on(':').trimResults().omitEmptyStrings().splitToList(schema.getProperty(META_TABLE_COLUMN_TYPES, "")).stream()
                    .map(hiveType -> HiveType.getType(hiveType, typeManager))
                    .collect(toList());

            // verify we can write all input columns to the file
            Set<Object> missingColumns = Sets.difference(new HashSet<>(inputColumnNames), new HashSet<>(fileColumnNames));
            if (!missingColumns.isEmpty()) {
                throw new PrestoException(HIVE_WRITER_ERROR, format("Table %s.%s does not have columns %s", schema, tableName, missingColumns));
            }
            if (fileColumnNames.size() != fileColumnTypes.size()) {
                throw new PrestoException(HIVE_INVALID_METADATA, format("Table '%s.%s' has metadata for column names of types", schemaName, tableName));
            }

            fieldCount = fileColumnNames.size();

            serializer = initializeSerializer(conf, schema, serDe);
            recordWriter = HiveWriteUtils.createRecordWriter(new Path(writePath, fileName), conf, schema, outputFormat);

            tableInspector = getStandardStructObjectInspector(fileColumnNames, getJavaObjectInspectors(fileColumnTypes));

            // reorder (and possibly reduce) struct fields to match input
            structFields = ImmutableList.copyOf(inputColumnNames.stream()
                    .map(tableInspector::getStructFieldRef)
                    .collect(toList()));

            row = tableInspector.create();
        }

        public void addRow(List<Object> fieldValues)
        {
            checkState(fieldValues.size() == fieldCount, "Invalid row");

            for (int field = 0; field < fieldCount; field++) {
                tableInspector.setStructFieldData(row, structFields.get(field), fieldValues.get(field));
            }

            try {
                recordWriter.write(serializer.serialize(row, tableInspector));
            }
            catch (SerDeException | IOException e) {
                throw new PrestoException(HIVE_WRITER_ERROR, e);
            }
        }

        public void commit()
        {
            try {
                recordWriter.close(false);
            }
            catch (IOException e) {
                throw new PrestoException(HIVE_WRITER_ERROR, "Error committing write to Hive", e);
            }
        }

        public void rollback()
        {
            try {
                recordWriter.close(true);
            }
            catch (IOException e) {
                throw new PrestoException(HIVE_WRITER_ERROR, "Error rolling back write to Hive", e);
            }
        }

        @SuppressWarnings("deprecation")
        private static Serializer initializeSerializer(Configuration conf, Properties properties, String serializerName)
        {
            try {
                Serializer result = (Serializer) Class.forName(serializerName).getConstructor().newInstance();
                result.initialize(conf, properties);
                return result;
            }
            catch (SerDeException | ReflectiveOperationException e) {
                throw Throwables.propagate(e);
            }
        }

        @Override
        public String toString()
        {
            return toStringHelper(this)
                    .add("writePath", writePath)
                    .add("fileName", fileName)
                    .toString();
        }
    }
}
