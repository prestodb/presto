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
package com.facebook.presto.raptor.storage;

import com.facebook.presto.spi.PrestoException;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.mapred.JobConf;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.raptor.RaptorErrorCode.RAPTOR_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.google.common.base.Functions.toStringFunction;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.FluentIterable.from;
import static com.google.common.collect.Iterables.transform;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_ORC_DEFAULT_COMPRESS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMNS;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.META_TABLE_COLUMN_TYPES;
import static org.apache.hadoop.hive.ql.io.orc.CompressionKind.SNAPPY;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory.getStandardStructObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;

public class OrcRowSink
        implements RowSink
{
    private static final JobConf JOB_CONF = new JobConf();

    private final int fieldCount;
    private final OrcSerde serializer;
    private final RecordWriter recordWriter;
    private final SettableStructObjectInspector tableInspector;
    private final List<StructField> structFields;
    private final Object row;
    private final int sampleWeightField;

    private int field = -1;

    public OrcRowSink(List<Long> columnIds, List<StorageType> columnTypes, Optional<Long> sampleWeightColumnId, File target)
    {
        checkArgument(columnIds.size() == columnTypes.size(), "ids and types mismatch");
        checkArgument(isUnique(columnIds), "ids must be unique");

        fieldCount = columnIds.size();
        sampleWeightField = columnIds.indexOf(sampleWeightColumnId.or(-1L));

        Iterable<String> hiveTypeNames = ImmutableList.copyOf(transform(columnTypes, hiveTypeName()));
        List<String> columnNames = ImmutableList.copyOf(transform(columnIds, toStringFunction()));

        Properties properties = new Properties();
        properties.setProperty(META_TABLE_COLUMNS, Joiner.on(',').join(columnNames));
        properties.setProperty(META_TABLE_COLUMN_TYPES, Joiner.on(':').join(hiveTypeNames));
        properties.setProperty(HIVE_ORC_DEFAULT_COMPRESS.name(), SNAPPY.name());

        serializer = createSerializer(JOB_CONF, properties);
        recordWriter = createRecordWriter(new Path(target.toURI()), JOB_CONF, properties);

        tableInspector = getStandardStructObjectInspector(columnNames, getJavaObjectInspectors(columnTypes));
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
        field = (sampleWeightField == 0) ? 1 : 0;
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
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to write record", e);
        }
    }

    @Override
    public int currentField()
    {
        checkState(field != -1, "not in record");
        return field;
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
    public void appendString(String value)
    {
        append(value);
    }

    @Override
    public void appendBytes(byte[] value)
    {
        append(value);
    }

    @Override
    public void close()
    {
        checkState(field == -1, "record not finished");

        try {
            recordWriter.close(false);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to close writer", e);
        }
    }

    private void append(Object value)
    {
        checkState(field != -1, "not in record");
        checkState(field < fieldCount, "all fields already set");

        tableInspector.setStructFieldData(row, structFields.get(field), value);
        field++;
        if (field == sampleWeightField) {
            field++;
        }
    }

    private static OrcSerde createSerializer(Configuration conf, Properties properties)
    {
        OrcSerde serde = new OrcSerde();
        serde.initialize(conf, properties);
        return serde;
    }

    private static RecordWriter createRecordWriter(Path target, JobConf conf, Properties properties)
    {
        try {
            return new OrcOutputFormat().getHiveRecordWriter(conf, target, null, false, properties, null);
        }
        catch (IOException e) {
            throw new PrestoException(RAPTOR_ERROR, "Failed to create writer", e);
        }
    }

    private static List<ObjectInspector> getJavaObjectInspectors(List<StorageType> types)
    {
        return from(types)
                .transform(new Function<StorageType, ObjectInspector>()
                {
                    @Override
                    public ObjectInspector apply(StorageType type)
                    {
                        return getJavaObjectInspector(type);
                    }
                })
                .toList();
    }

    private static ObjectInspector getJavaObjectInspector(StorageType type)
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
            case BYTES:
                return javaByteArrayObjectInspector;
        }
        throw new PrestoException(INTERNAL_ERROR, "Unhandled storage type: " + type);
    }

    private static Function<StorageType, String> hiveTypeName()
    {
        return new Function<StorageType, String>()
        {
            @Override
            public String apply(StorageType type)
            {
                return type.getHiveTypeName();
            }
        };
    }

    private static <T> boolean isUnique(Collection<T> items)
    {
        return new HashSet<>(items).size() == items.size();
    }
}
