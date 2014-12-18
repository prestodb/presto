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

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordSink;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.Serializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.facebook.presto.hive.HiveUtil.isArrayType;
import static com.facebook.presto.hive.HiveUtil.isMapType;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.transform;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;

public class HiveRecordSink
        implements RecordSink
{
    private final int fieldCount;
    @SuppressWarnings("deprecation")
    private final Serializer serializer;
    private final RecordWriter recordWriter;
    private final SettableStructObjectInspector tableInspector;
    private final List<StructField> structFields;
    private final List<Type> columnTypes;
    private final Object row;
    private final int sampleWeightField;
    private final ConnectorSession connectorSession;

    private int field = -1;

    public HiveRecordSink(HiveOutputTableHandle handle, Path target, JobConf conf)
    {
        fieldCount = handle.getColumnNames().size();

        sampleWeightField = handle.getColumnNames().indexOf(SAMPLE_WEIGHT_COLUMN_NAME);
        columnTypes = ImmutableList.copyOf(handle.getColumnTypes());
        connectorSession = handle.getConnectorSession();

        Iterable<String> hiveTypeNames = transform(transform(handle.getColumnTypes(), HiveType::toHiveType), HiveType::getHiveTypeName);

        Properties properties = new Properties();
        properties.setProperty(META_TABLE_COLUMNS, Joiner.on(',').join(handle.getColumnNames()));
        properties.setProperty(META_TABLE_COLUMN_TYPES, Joiner.on(':').join(hiveTypeNames));

        serializer = initializeSerializer(conf, properties, handle.getHiveStorageFormat().getSerDe());
        recordWriter = createRecordWriter(target, conf, properties, handle.getHiveStorageFormat().getOutputFormat());

        tableInspector = getStandardStructObjectInspector(handle.getColumnNames(), getJavaObjectInspectors(columnTypes));
        structFields = ImmutableList.copyOf(tableInspector.getAllStructFieldRefs());
        row = tableInspector.create();
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

        try {
            recordWriter.write(serializer.serialize(row, tableInspector));
        }
        catch (SerDeException | IOException e) {
            throw Throwables.propagate(e);
        }
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
        Type type = columnTypes.get(field);
        if (type == DateType.DATE) {
            // todo should this be adjusted to midnight in JVM timezone?
            append(new Date(TimeUnit.DAYS.toMillis(value)));
        }
        else if (type == TimestampType.TIMESTAMP) {
            append(new Timestamp(value));
        }
        else {
            append(value);
        }
    }

    @Override
    public void appendDouble(double value)
    {
        append(value);
    }

    @Override
    public void appendString(byte[] value)
    {
        Type type = columnTypes.get(field);
        if (type == VarbinaryType.VARBINARY) {
            append(value);
        }
        else {
            append(new String(value, UTF_8));
        }
    }

    @Override
    public String commit()
    {
        checkState(field == -1, "record not finished");

        try {
            recordWriter.close(false);
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return ""; // the committer can list the directory
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    private void append(Object value)
    {
        checkState(field != -1, "not in record");
        checkState(field < fieldCount, "all fields already set");

        Type type = columnTypes.get(field);
        if (isMapType(type) || isArrayType(type)) {
            // Hive expects a List<>/Map<> to write, so decode the value
            value = TypeJsonUtils.stackRepresentationToObject(connectorSession, (String) value, type);
        }
        tableInspector.setStructFieldData(row, structFields.get(field), value);
        field++;
        if (field == sampleWeightField) {
            field++;
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

    private static RecordWriter createRecordWriter(Path target, JobConf conf, Properties properties, String outputFormatName)
    {
        try {
            Object writer = Class.forName(outputFormatName).getConstructor().newInstance();
            return ((HiveOutputFormat<?, ?>) writer).getHiveRecordWriter(conf, target, Text.class, false, properties, Reporter.NULL);
        }
        catch (IOException | ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    private static List<ObjectInspector> getJavaObjectInspectors(Iterable<Type> types)
    {
        ImmutableList.Builder<ObjectInspector> list = ImmutableList.builder();
        for (Type type : types) {
            list.add(getJavaObjectInspector(type));
        }
        return list.build();
    }

    private static ObjectInspector getJavaObjectInspector(Type type)
    {
        if (type.equals(BooleanType.BOOLEAN)) {
            return javaBooleanObjectInspector;
        }
        else if (type.equals(BigintType.BIGINT)) {
            return javaLongObjectInspector;
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return javaDoubleObjectInspector;
        }
        else if (type.equals(VarcharType.VARCHAR)) {
            return javaStringObjectInspector;
        }
        else if (type.equals(VarbinaryType.VARBINARY)) {
            return javaByteArrayObjectInspector;
        }
        else if (type.equals(DateType.DATE)) {
            return javaDateObjectInspector;
        }
        else if (type.equals(TimestampType.TIMESTAMP)) {
            return javaTimestampObjectInspector;
        }
        else if (isArrayType(type)) {
            return ObjectInspectorFactory.getStandardListObjectInspector(getJavaObjectInspector(type.getTypeParameters().get(0)));
        }
        else if (isMapType(type)) {
            ObjectInspector keyObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(0));
            ObjectInspector valueObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(1));
            return ObjectInspectorFactory.getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }
}
