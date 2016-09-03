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

import com.facebook.presto.hive.HiveWriteUtils.FieldSetter;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe;
import org.apache.hadoop.hive.serde2.columnar.OptimizedLazyBinaryColumnarSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_CLOSE_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_WRITER_DATA_ERROR;
import static com.facebook.presto.hive.HiveType.toHiveTypes;
import static com.facebook.presto.hive.HiveWriteUtils.createFieldSetter;
import static com.facebook.presto.hive.HiveWriteUtils.createRecordWriter;
import static com.facebook.presto.hive.HiveWriteUtils.getRowColumnInspectors;
import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;

public class HiveRecordWriter
{
    private final Path path;
    private final int fieldCount;
    @SuppressWarnings("deprecation")
    private final Serializer serializer;
    private final RecordWriter recordWriter;
    private final SettableStructObjectInspector tableInspector;
    private final List<StructField> structFields;
    private final Object row;
    private final FieldSetter[] setters;

    public HiveRecordWriter(
            Path path,
            List<String> inputColumnNames,
            StorageFormat storageFormat,
            Properties schema,
            TypeManager typeManager,
            JobConf conf)
    {
        this.path = requireNonNull(path, "path is null");

        // existing tables may have columns in a different order
        List<String> fileColumnNames = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(schema.getProperty(META_TABLE_COLUMNS, ""));
        List<Type> fileColumnTypes = toHiveTypes(schema.getProperty(META_TABLE_COLUMN_TYPES, "")).stream()
                .map(hiveType -> hiveType.getType(typeManager))
                .collect(toList());

        fieldCount = fileColumnNames.size();

        String serDe = storageFormat.getSerDe();
        if (serDe.equals(LazyBinaryColumnarSerDe.class.getName())) {
            serDe = OptimizedLazyBinaryColumnarSerde.class.getName();
        }
        serializer = initializeSerializer(conf, schema, serDe);
        recordWriter = createRecordWriter(path, conf, schema, storageFormat.getOutputFormat());

        List<ObjectInspector> objectInspectors = getRowColumnInspectors(fileColumnTypes);
        tableInspector = getStandardStructObjectInspector(fileColumnNames, objectInspectors);

        // reorder (and possibly reduce) struct fields to match input
        structFields = ImmutableList.copyOf(inputColumnNames.stream()
                .map(tableInspector::getStructFieldRef)
                .collect(toList()));

        row = tableInspector.create();

        setters = new FieldSetter[structFields.size()];
        for (int i = 0; i < setters.length; i++) {
            setters[i] = createFieldSetter(tableInspector, row, structFields.get(i), fileColumnTypes.get(structFields.get(i).getFieldID()));
        }
    }

    public void addRow(Block[] columns, int position)
    {
        for (int field = 0; field < fieldCount; field++) {
            if (columns[field].isNull(position)) {
                tableInspector.setStructFieldData(row, structFields.get(field), null);
            }
            else {
                setters[field].setField(columns[field], position);
            }
        }

        try {
            recordWriter.write(serializer.serialize(row, tableInspector));
        }
        catch (SerDeException | IOException e) {
            throw new PrestoException(HIVE_WRITER_DATA_ERROR, e);
        }
    }

    public void commit()
    {
        try {
            recordWriter.close(false);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error committing write to Hive", e);
        }
    }

    public void rollback()
    {
        try {
            recordWriter.close(true);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_WRITER_CLOSE_ERROR, "Error rolling back write to Hive", e);
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
                .add("path", path)
                .toString();
    }
}
