
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
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import static java.util.UUID.randomUUID;
import static com.facebook.presto.hive.HiveType.columnTypeToHiveType;
import static com.facebook.presto.hive.HiveType.hiveTypeNameGetter;
import static com.facebook.presto.hive.HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.google.common.base.Preconditions.checkState;
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

public class HiveRecordSink
        implements RecordSink
{
    private final int fieldCount;
    private final int dataFieldsCount;
    @SuppressWarnings("deprecation")
    private final Serializer serializer;
    private final RecordWriter recordWriter;
    private final SettableStructObjectInspector tableInspector;
    private final List<StructField> structFields;
    private final Object row;
    private final int sampleWeightField;

    private final Path basePath;
    private final String fileName;
    private final String filePrefix;

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
        fieldCount = handle.getColumnNames().size();
        dataFieldsCount = handle.getDataColumnNames().size();
        this.conf = conf;

        sampleWeightField = handle.getColumnNames().indexOf(SAMPLE_WEIGHT_COLUMN_NAME);

        Iterable<HiveType> hiveTypes = transform(handle.getDataColumnTypes(), columnTypeToHiveType());
        Iterable<String> hiveTypeNames = transform(hiveTypes, hiveTypeNameGetter());

        properties = new Properties();
        properties.setProperty(META_TABLE_COLUMNS, Joiner.on(',').join(handle.getDataColumnNames()));
        properties.setProperty(META_TABLE_COLUMN_TYPES, Joiner.on(':').join(hiveTypeNames));

        Map<String, String> serdeParameters = handle.getSerdeParameters();
        if (serdeParameters != null) {
            for (String key : serdeParameters.keySet()) {
                properties.setProperty(key, serdeParameters.get(key));
            }
        }

        try {
            serializer = ReflectionUtils.newInstance(conf.getClassByName(handle.getSerdeLib()).
                                         asSubclass(Serializer.class), conf);
            Class<?> clazz = Class.forName(handle.getOutputFormat());
            outputFormatClass = clazz.asSubclass(HiveOutputFormat.class);
        }
        catch (ClassNotFoundException | ClassCastException e) {
            throw Throwables.propagate(e);
        }

        initializeSerializer(conf, properties, serializer);

        isPartitioned = handle.isOutputTablePartitioned();
        filePrefix = handle.getFilePrefix();
        fileName = filePrefix + "_" + randomUUID().toString();
        basePath = target;
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
            partitionColNames = handle.getPartitionColumnNames();
            partitionValues = new ArrayList<String>(partitionColNames.size());

            recordWriter = null;
        }
        else {
            Path filePath = new Path(target, fileName);
            recordWriter = createRecordWriter(filePath, conf, properties, getOutputFormatInstance());
        }

        tableInspector = getStandardStructObjectInspector(handle.getDataColumnNames(), getJavaObjectInspectors(hiveTypes));
        structFields = ImmutableList.copyOf(tableInspector.getAllStructFieldRefs());
        row = tableInspector.create();
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

        if (isPartitioned) {
            String pathName = FileUtils.makePartName(partitionColNames, partitionValues);
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

    private RecordWriter getRecordWriter(String pathName)
    {
        checkNotNull(basePath, "Base path for table data not set");

        Path partitionPath = new Path(basePath, (pathName.startsWith("/") ? pathName.substring(1) : pathName));
        Path filePath = new Path(partitionPath, fileName);
        return createRecordWriter(filePath, conf, properties, getOutputFormatInstance());
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

        try {
            if (isPartitioned) {
                for (String path : recordWriters.asMap().keySet()) {
                    recordWriters.get(path).close(false);
                }
            }
            else {
                recordWriter.close(false);
            }
        }
        catch (IOException | ExecutionException e) {
            throw Throwables.propagate(e);
        }

        return ""; // the committer can list the directory
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
        switch (type) {
            case BOOLEAN:
                return javaBooleanObjectInspector;
            case LONG:
                return javaLongObjectInspector;
            case DOUBLE:
                return javaDoubleObjectInspector;
            case STRING:
                return javaStringObjectInspector;
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }
}
