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

import com.facebook.presto.spi.RecordSink;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.util.ReflectionUtils;

import com.facebook.presto.spi.type.Type;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.hive.serde2.Deserializer;

import java.io.IOException;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveType.columnTypeToHiveType;
import static com.facebook.presto.hive.HiveType.hiveTypeNameGetter;
import static com.facebook.presto.hive.HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.google.common.base.Preconditions.checkState;

import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.concurrent.ExecutionException;

import static java.util.UUID.randomUUID;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.collect.Iterables.transform;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

import static com.facebook.presto.hive.HivePartition.UNPARTITIONED_ID;

public class HiveRecordSink
        implements RecordSink
{
    private final int fieldCount;
    private final int dataFieldsCount;
    @SuppressWarnings("deprecation")
    private final Serializer serializer;
    private RecordWriter recordWriter;
    private final SettableStructObjectInspector tableInspector;
    private final List<StructField> structFields;
    private final Object row;
    private final int sampleWeightField;

    private final Path basePath;
    private final String fileName;

    private Map<String, List<String>> filesWritten; // filesWritten for each partition

    private final JobConf conf;
    private final Properties properties;
    private LoadingCache<String, RecordWriter> recordWriters;
    Class<? extends HiveOutputFormat> outputFormatClass = null;

    private final boolean isPartitioned;

    private int field = -1;

    private List<String> partitionColNames;
    private List<String> partitionValues;

    public HiveRecordSink(HiveOutputTableHandle handle, Path target, JobConf conf)
    {
        this(target,
        conf,
        handle.getColumnNames(),
        handle.getColumnNames(), // CTAS doesnt support partitioning => all cols are data columns
        handle.getColumnTypes(),
        handle.getColumnTypes(),
        "org.apache.hadoop.hive.ql.io.RCFileOutputFormat",
        "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe",
        null,
        "",
        false);
    }

    public HiveRecordSink(HiveInsertTableHandle handle, Path target, JobConf conf)
    {
        this(target,
            conf,
            handle.getColumnNames(),
            handle.getDataColumnNames(),
            handle.getColumnTypes(),
            handle.getDataColumnTypes(),
            handle.getOutputFormat(),
            handle.getSerdeLib(),
            handle.getSerdeParameters(),
            handle.getFilePrefix(),
            handle.isOutputTablePartitioned());

        if (isPartitioned) {
            partitionColNames = handle.getPartitionColumnNames();
            partitionValues = new ArrayList<String>(partitionColNames.size());
        }
    }

    private HiveRecordSink(Path target,
            JobConf conf,
            List<String> columnNames,
            List<String> dataColumnNames,
            List<Type> columnTypes,
            List<Type> dataColumnTypes,
            String outputFormat,
            String serdeLib,
            Map<String, String> serdeParameters,
            String filePrefix,
            boolean isPartitioned)
    {
        fieldCount = columnNames.size();
        dataFieldsCount = dataColumnNames.size();
        this.conf = conf;
        this.isPartitioned = isPartitioned;

        sampleWeightField = columnNames.indexOf(SAMPLE_WEIGHT_COLUMN_NAME);

        Iterable<HiveType> hiveTypes = transform(dataColumnTypes, columnTypeToHiveType());
        Iterable<String> hiveTypeNames = transform(hiveTypes, hiveTypeNameGetter());

        properties = new Properties();
        properties.setProperty(META_TABLE_COLUMNS, Joiner.on(',').join(dataColumnNames));
        properties.setProperty(META_TABLE_COLUMN_TYPES, Joiner.on(':').join(hiveTypeNames));

        if (serdeParameters != null) {
            for (String key : serdeParameters.keySet()) {
                properties.setProperty(key, serdeParameters.get(key));
            }
        }

        try {
            serializer = (Serializer) lookupDeserializer(serdeLib);
            Class<?> clazz = Class.forName(outputFormat);
            outputFormatClass = clazz.asSubclass(HiveOutputFormat.class);
        }
        catch (ClassNotFoundException | ClassCastException e) {
            throw Throwables.propagate(e);
        }
        initializeSerializer(conf, properties, serializer);

        filePrefix = (filePrefix.length() > 0) ? filePrefix + "_" : filePrefix;
        fileName = filePrefix + randomUUID().toString();
        basePath = target;
        filesWritten = new HashMap<String, List<String>>();
        if (isPartitioned) {
            recordWriters = CacheBuilder.newBuilder()
                    .build(
                        new CacheLoader<String, RecordWriter>() {
                            @Override
                            public RecordWriter load(String path)
                            {
                                return getRecordWriter(path);
                            }
                        });
            recordWriter = null;
        }
        else {
            createNonPartitionedRecordReader();
        }

        tableInspector = getStandardStructObjectInspector(dataColumnNames, getJavaObjectInspectors(hiveTypes));
        structFields = ImmutableList.copyOf(tableInspector.getAllStructFieldRefs());
        row = tableInspector.create();
    }

    private RecordWriter createNonPartitionedRecordReader()
    {
        String name = getFileName();
        Path filePath = new Path(basePath, name);
        if (!filesWritten.containsKey(UNPARTITIONED_ID)) {
            filesWritten.put(UNPARTITIONED_ID, new ArrayList<String>());
        }

        filesWritten.get(UNPARTITIONED_ID).add(name);

        recordWriter = createRecordWriter(filePath, conf, properties, getOutputFormatInstance());
        return recordWriter;
    }

    private HiveOutputFormat<?, ?> getOutputFormatInstance()
    {
        HiveOutputFormat<?, ?> outputFormat;
        try {
            outputFormat = outputFormatClass.newInstance();
        }
        catch (InstantiationException | IllegalAccessException e) {
            throw Throwables.propagate(e);
        }

        return outputFormat;
    }

    @Override
    public void beginRecord(long sampleWeight)
    {
        checkState(field == -1, "already in record");
        if (sampleWeightField >= 0) {
            tableInspector.setStructFieldData(row, structFields.get(sampleWeightField), sampleWeight);
        }
        field = 0;
        if (sampleWeightField == 0) {
            field++;
        }
    }

    @Override
    public void finishRecord()
    {
        checkState(field != -1, "not in record");
        checkState(field == fieldCount, "not all fields set");
        field = -1;

        RecordWriter rw = recordWriter;
        String partition = UNPARTITIONED_ID;

        if (isPartitioned) {
            String pathName = FileUtils.makePartName(partitionColNames, partitionValues);
            partition = pathName;
            try {
                rw = recordWriters.get(pathName);
            }
            catch (ExecutionException e) {
                throw Throwables.propagate(e);
            }
            partitionValues.clear();
        }

        try {
            rw.write(serializer.serialize(row, tableInspector));
        }
        catch (SerDeException | IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private String getFileName()
    {
        return fileName;
    }

    private RecordWriter getRecordWriter(String pathName)
    {
        checkNotNull(basePath, "Base path for table data not set");
        String partitionId = pathName.startsWith("/") ? pathName.substring(1) : pathName;
        Path partitionPath = new Path(basePath, partitionId);
        String name = getFileName();
        Path filePath = new Path(partitionPath, name);
        if (!filesWritten.containsKey(partitionId)) {
            filesWritten.put(partitionId, new ArrayList<String>());
        }

        filesWritten.get(partitionId).add(name);

        return createRecordWriter(filePath, conf, properties, getOutputFormatInstance());
    }

    private Deserializer lookupDeserializer(String lib) throws ClassNotFoundException
    {
        Deserializer deserializer = ReflectionUtils.newInstance(conf.getClassByName(lib).
                                                    asSubclass(Deserializer.class), conf);
        return deserializer;
    }

    @Override
    public void appendNull()
    {
        append(null);
    }

    @Override
    public void appendBoolean(boolean value)
    {
        append(value);
    }

    @Override
    public void appendLong(long value)
    {
        append(value);
    }

    @Override
    public void appendDouble(double value)
    {
        append(value);
    }

    @Override
    public void appendString(byte[] value)
    {
        append(new String(value, UTF_8));
    }

    @Override
    public String commit()
    {
        checkState(field == -1, "record not finished");
        String partitionsJson = "";

        try {
            if (isPartitioned) {
                for (String path : recordWriters.asMap().keySet()) {
                    recordWriters.get(path).close(false);
                }
            }
            else {
                recordWriter.close(false);
            }
            ObjectMapper mapper = new ObjectMapper();
            partitionsJson = mapper.writeValueAsString(filesWritten);
        }
        catch (IOException | ExecutionException e) {
            throw Throwables.propagate(e);
        }

        return partitionsJson; // partition list will be used in commit of insert to add partitions
    }

    private void append(Object value)
    {
        checkState(field != -1, "not in record");
        checkState(field < fieldCount, "all fields already set");

        if (field < dataFieldsCount) {
            tableInspector.setStructFieldData(row, structFields.get(field), value);
            field++;
            if (field == sampleWeightField) {
                field++;
            }
        }
        else {
            // into partition columns now
            if (value != null) {
                partitionValues.add(value.toString());
            }
            else {
                throw new RuntimeException(String.format("Null value for partition column '%s'", partitionColNames.get(field - dataFieldsCount)));
            }
            field++;
            checkState(field != sampleWeightField, "Partition columns not at the end");
        }
    }

    @SuppressWarnings("deprecation")
    private static Serializer initializeSerializer(Configuration conf, Properties properties, Serializer serializer)
    {
        try {
            serializer.initialize(conf, properties);
        }
        catch (SerDeException e) {
            throw Throwables.propagate(e);
        }
        return serializer;
    }

    private static RecordWriter createRecordWriter(Path target, JobConf conf, Properties properties, HiveOutputFormat<?, ?> outputFormat)
    {
        try {
            return outputFormat.getHiveRecordWriter(conf, target, Text.class, false, properties, Reporter.NULL);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static List<ObjectInspector> getJavaObjectInspectors(Iterable<HiveType> hiveTypes)
    {
        ImmutableList.Builder<ObjectInspector> list = ImmutableList.builder();
        for (HiveType type : hiveTypes) {
            list.add(getJavaObjectInspector(type));
        }
        return list.build();
    }

    private static PrimitiveObjectInspector getJavaObjectInspector(HiveType type)
    {
        if (type.equals(HiveType.HIVE_BOOLEAN)) {
            return javaBooleanObjectInspector;
        }
        else if (type.equals(HiveType.HIVE_LONG)) {
            return javaLongObjectInspector;
        }
        else if (type.equals(HiveType.HIVE_DOUBLE)) {
            return javaDoubleObjectInspector;
        }
        else if (type.equals(HiveType.HIVE_STRING)) {
            return javaStringObjectInspector;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }
}
